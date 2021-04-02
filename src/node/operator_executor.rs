use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::prelude::*,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use futures::future;
use slog::{Drain, Logger};
use tokio::{
    self,
    stream::{Stream, StreamExt, StreamMap},
    sync::{broadcast, mpsc, watch, Mutex},
};

use crate::{
    communication::{ControlMessage, RecvEndpoint},
    dataflow::{
        operator::{Operator, OperatorConfig},
        stream::{InternalReadStream, StreamId},
        Data, EventMakerT, Message, ReadStream,
    },
    deadlines::{Deadline, Notification},
    node::lattice::ExecutionLattice,
    node::operator_event::OperatorEvent,
    Uuid,
};

#[derive(Clone, Debug, PartialEq)]
enum EventRunnerMessage {
    AddedEvents,
    DestroyOperator,
}

pub trait OperatorExecutorStreamT: Send + Stream<Item = Vec<OperatorEvent>> {
    fn get_id(&self) -> StreamId;
    fn get_closed_ref(&self) -> Arc<AtomicBool>;
    fn to_pinned_stream(self: Box<Self>) -> Pin<Box<dyn Send + Stream<Item = Vec<OperatorEvent>>>>;
}

pub struct OperatorExecutorStream<D: Data> {
    stream: Rc<RefCell<InternalReadStream<D>>>,
    recv_endpoint: Option<RecvEndpoint<Arc<Message<D>>>>,
    closed: Arc<AtomicBool>,
}

impl<D: Data> OperatorExecutorStreamT for OperatorExecutorStream<D> {
    fn get_id(&self) -> StreamId {
        self.stream.borrow().get_id()
    }

    fn get_closed_ref(&self) -> Arc<AtomicBool> {
        self.closed.clone()
    }

    fn to_pinned_stream(self: Box<Self>) -> Pin<Box<dyn Send + Stream<Item = Vec<OperatorEvent>>>> {
        Box::into_pin(self as Box<dyn Send + Stream<Item = Vec<OperatorEvent>>>)
    }
}

impl<D: Data> Stream for OperatorExecutorStream<D> {
    // Item = OperatorEvent might be better?
    type Item = Vec<OperatorEvent>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Vec<OperatorEvent>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }
        let mut mut_self = self.as_mut();
        if mut_self.recv_endpoint.is_none() {
            let endpoint = mut_self.stream.borrow_mut().take_endpoint();
            mut_self.recv_endpoint = endpoint;
        }
        match mut_self.recv_endpoint.as_mut() {
            Some(RecvEndpoint::InterThread(rx)) => match rx.poll_recv(cx) {
                Poll::Ready(Some(msg)) => {
                    if msg.is_top_watermark() {
                        self.closed.store(true, Ordering::SeqCst);
                        self.recv_endpoint = None;
                    }
                    Poll::Ready(Some(self.stream.borrow().make_events(msg)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            None => Poll::Ready(None),
        }
    }
}

unsafe impl<D: Data> Send for OperatorExecutorStream<D> {}

impl<D: Data> From<Rc<RefCell<InternalReadStream<D>>>> for OperatorExecutorStream<D> {
    fn from(stream: Rc<RefCell<InternalReadStream<D>>>) -> Self {
        Self::new(stream)
    }
}

impl<D: Data> From<&ReadStream<D>> for OperatorExecutorStream<D> {
    fn from(stream: &ReadStream<D>) -> Self {
        let internal_stream: Rc<RefCell<InternalReadStream<D>>> = stream.into();
        Self::from(internal_stream)
    }
}

impl<D: Data> OperatorExecutorStream<D> {
    pub fn new(stream: Rc<RefCell<InternalReadStream<D>>>) -> Self {
        let closed = Arc::new(AtomicBool::new(stream.borrow().is_closed()));
        Self {
            stream,
            recv_endpoint: None,
            closed,
        }
    }
}

/// `OperatorExecutor` is a structure that is in charge of executing callbacks associated with
/// messages and watermarks arriving on input streams at an `Operator`. The callbacks are invoked
/// according to the partial order defined in [`OperatorEvent`].
///
/// The `event_runner` function is in charge of executing the callbacks until it receives a
/// `DestroyOperator` message. The `execute` function retrieves messages from the input streams and
/// sends an `AddedEvents` message to the `event_runner` invocations to make them process the
/// events.
pub struct OperatorExecutor {
    /// The instance of the operator that needs to be executed.
    operator: Box<dyn Operator>,
    /// The configuration with which the operator was instantiated, without the argument.
    config: OperatorConfig<()>,
    /// A merged stream of all the input streams of the operator. This is used to retrieve events
    /// to execute.
    event_stream: Option<Pin<Box<dyn Send + Stream<Item = Vec<OperatorEvent>>>>>,
    /// Used to decide whether to run destroy()
    streams_closed: HashMap<StreamId, Arc<AtomicBool>>,
    /// A lattice that keeps a partial order of the events that need to be processed.
    lattice: Arc<ExecutionLattice>,
    /// Receives control messages regarding the operator.
    control_rx: mpsc::UnboundedReceiver<ControlMessage>,
}

impl OperatorExecutor {
    /// Creates a new OperatorEvent.
    pub fn new<T: 'static + Operator, U: Clone>(
        operator: T,
        config: OperatorConfig<U>,
        mut operator_streams: Vec<Box<dyn OperatorExecutorStreamT>>,
        control_rx: mpsc::UnboundedReceiver<ControlMessage>,
    ) -> Self {
        let streams_closed: HashMap<_, _> = operator_streams
            .iter()
            .map(|s| (s.get_id(), s.get_closed_ref()))
            .collect();
        let event_stream = operator_streams.pop().map(|first| {
            operator_streams
                .into_iter()
                .fold(first.to_pinned_stream(), |x, y| {
                    Box::pin(StreamExt::merge(x, y.to_pinned_stream()))
                })
        });

        Self {
            operator: Box::new(operator),
            config: config.drop_arg(),
            event_stream,
            streams_closed,
            lattice: Arc::new(ExecutionLattice::new()),
            control_rx,
        }
    }

    /// Whether all input streams have been closed.
    ///
    /// Returns true if there are no input streams.
    fn all_streams_closed(&self) -> bool {
        self.streams_closed
            .values()
            .all(|x| x.load(Ordering::SeqCst))
    }

    /// A high-level execute function that first waits for a [`ControlMessage::RunOperator`] message
    /// and executes [`Operator::run`].
    /// Once [`Operator::run`] completes, the function runs callbacks by retrieving events from the
    /// input streams, adding them to the lattice maintained by the executor and notifying the
    /// `event_runner` invocations to process the received events.
    pub async fn execute(&mut self) {
        loop {
            if let Some(ControlMessage::RunOperator(id)) = self.control_rx.recv().await {
                if id == self.config.id {
                    break;
                }
            }
        }

        let name = self
            .config
            .name
            .clone()
            .unwrap_or_else(|| format!("{}", self.config.id));
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            name
        );

        // Callbacks are not invoked while the operator is running.
        tokio::task::block_in_place(|| self.operator.run());

        // Deadlines.
        let file = if self.config.logging {
            fs::create_dir_all("/tmp/erdos").unwrap();
            Some(Arc::new(Mutex::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(format!("/tmp/erdos/{}.log", self.config.id))
                    .unwrap(),
            )))
        } else {
            None
        };

        let _deadline_task_handles: Vec<_> = {
            let mut deadlines = self.config.deadlines.try_lock().unwrap();
            deadlines
                .drain(..)
                .map(|deadline| tokio::spawn(Self::enforce_deadline(deadline, file.clone())))
                .collect()
        };

        if let Some(mut event_stream) = self.event_stream.take() {
            // Launch consumers
            // TODO: use CondVar instead of watch.
            // TODO: adjust number of event runners. based on size of event lattice.
            let (notifier_tx, notifier_rx) = watch::channel(EventRunnerMessage::AddedEvents);
            let mut event_runner_handles = Vec::new();
            for _ in 0..self.config.num_event_runners {
                let event_runner_fut =
                    Self::event_runner(Arc::clone(&self.lattice), notifier_rx.clone());
                event_runner_handles.push(tokio::spawn(event_runner_fut));
            }
            while let Some(events) = event_stream.next().await {
                {
                    // Add all the received events to the lattice.
                    self.lattice.add_events(events).await;
                    // Notify receivers that new events were added.
                    notifier_tx
                        .broadcast(EventRunnerMessage::AddedEvents)
                        .unwrap();
                }
            }
            // Wait for event runners to finish.
            notifier_tx
                .broadcast(EventRunnerMessage::DestroyOperator)
                .unwrap();
            // Handle errors?
            future::join_all(event_runner_handles).await;
        }

        if self.all_streams_closed() {
            slog::debug!(
                crate::TERMINAL_LOGGER,
                "Node {}: destroying operator {}",
                self.config.node_id,
                name,
            );
            self.operator.destroy();
        }
    }

    // Spawn 1 task for each deadline. This is bad.
    async fn enforce_deadline(
        mut deadline: Box<dyn Deadline + Send>,
        file: Option<Arc<Mutex<File>>>,
    ) {
        let mut start_condition_channels = StreamMap::new();
        for (i, rx) in deadline
            .get_start_condition_receivers()
            .into_iter()
            .enumerate()
        {
            start_condition_channels.insert(i, rx);
        }

        let mut end_condition_channels = StreamMap::new();
        for (i, rx) in deadline
            .get_end_condition_receivers()
            .into_iter()
            .enumerate()
        {
            end_condition_channels.insert(i, rx);
        }

        // Contains handlers.
        let mut active_deadlines: tokio::time::DelayQueue<(
            Uuid,
            Instant,
            Arc<dyn Send + Sync + Fn() -> String>,
        )> = tokio::time::DelayQueue::new();
        // Pairs of (key in active_deadlines, end_condition).
        let mut end_conditions: Vec<(
            Uuid,
            tokio::time::delay_queue::Key,
            Arc<dyn Send + Sync + FnMut(&Notification) -> bool>,
        )> = Vec::new();
        loop {
            tokio::select! {
                // Start condition notification.
                msg = start_condition_channels.next() => {
                    let notification = msg.unwrap().1.unwrap();
                    if let Some((instant, end_condition, handler)) = deadline.start_condition(&notification) {
                        let id = Uuid::new_deterministic();
                        let key = active_deadlines.insert_at((id, instant, handler), tokio::time::Instant::from_std(instant));
                        end_conditions.push((id, key, end_condition));
                    }
                }
                // End condition notification.
                msg = end_condition_channels.next() => {
                    let notification = msg.unwrap().1.unwrap();
                    end_conditions = end_conditions.into_iter().filter_map(|(id, key, mut end_condition_arc)| {
                        let end_condition = Arc::get_mut(&mut end_condition_arc).unwrap();
                        if end_condition(&notification) {
                            slog::error!(crate::TERMINAL_LOGGER, "Resolved end condition");
                            active_deadlines.remove(&key);
                            None
                        } else {
                            Some((id, key, end_condition_arc))
                        }
                    }).collect();
                }
                // Deadline missed.
                result = active_deadlines.next(), if !active_deadlines.is_empty() => {
                    let (id, instant, handler) = result.unwrap().unwrap().into_inner();
                    let mut now_duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    let now_instant = Instant::now();
                    let duration_to_deadline = now_instant.duration_since(instant);
                    let deadline_time = now_duration.as_secs_f64() - duration_to_deadline.as_secs_f64();

                    if let Some(file) = file.as_ref() {
                        let mut file = file.lock().await;
                        writeln!(file, "{}: invoking handler for deadline {} @ {}", deadline.description(), deadline_time, now_duration.as_secs_f64()).unwrap();
                        file.flush().unwrap();
                    }
                    let info = handler();
                    now_duration += now_instant.elapsed();
                    if let Some(file) = file.as_ref() {
                        let mut file = file.lock().await;
                        writeln!(file, "{}: finished invoking handler for deadline {} @ {}; {}", deadline.description(), deadline_time, now_duration.as_secs_f64(), info).unwrap();
                        file.flush().unwrap();
                    }
                    end_conditions.retain(|(other_id, _, _)| &id != other_id);
                }
            }
        }
    }

    /// An `event_runner` invocation is in charge of executing callbacks associated with an event.
    /// Upon receipt of an `AddedEvents` notification, it queries the lattice for events that are
    /// ready to run, executes them, and notifies the lattice of their completion.
    async fn event_runner(
        lattice: Arc<ExecutionLattice>,
        mut notifier_rx: watch::Receiver<EventRunnerMessage>,
    ) {
        // Wait for notification for events added.
        while let Some(control_msg) = notifier_rx.recv().await {
            while let Some((event, event_id)) = lattice.get_event().await {
                tokio::task::block_in_place(event.callback);
                lattice.mark_as_completed(event_id).await;
            }
            if EventRunnerMessage::DestroyOperator == control_msg {
                break;
            }
        }
    }
}

unsafe impl Send for OperatorExecutor {}
