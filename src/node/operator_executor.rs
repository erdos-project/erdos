use std::{
    cell::RefCell,
    collections::HashMap,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures::future;
use tokio::{
    self,
    stream::{Stream, StreamExt},
    sync::{mpsc, watch},
};

use crate::{
    communication::{ControlMessage, RecvEndpoint},
    dataflow::{
        operator::{Operator, OperatorConfig},
        stream::{InternalReadStream, StreamId},
        Data, EventMakerT, Message, ReadStream,
    },
    node::lattice::ExecutionLattice,
    node::operator_event::OperatorEvent,
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
    recv_endpoint: Option<RecvEndpoint<Message<D>>>,
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
    logger: slog::Logger,
}

impl OperatorExecutor {
    /// Creates a new OperatorEvent.
    pub fn new<T: 'static + Operator, U: Clone>(
        operator: T,
        config: OperatorConfig<U>,
        mut operator_streams: Vec<Box<dyn OperatorExecutorStreamT>>,
        control_rx: mpsc::UnboundedReceiver<ControlMessage>,
        logger: slog::Logger,
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
            logger,
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
            self.logger,
            "Node {}: running operator {}",
            self.config.node_id,
            name
        );

        // Callbacks are not invoked while the operator is running.
        self.operator.run();

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
                    {
                        // Add all the received events to the lattice.
                        for event in events {
                            self.lattice.add_event(event).await;
                        }
                    }
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

            if self.all_streams_closed() {
                slog::debug!(
                    self.logger,
                    "Node {}: destroying operator {}",
                    self.config.node_id,
                    name,
                );
                self.operator.destroy();
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
                (event.callback)();
                lattice.mark_as_completed(event_id).await;
            }
            if EventRunnerMessage::DestroyOperator == control_msg {
                break;
            }
        }
    }
}

unsafe impl Send for OperatorExecutor {}
