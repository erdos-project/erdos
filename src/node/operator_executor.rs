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

use tokio::{
    self,
    stream::{Stream, StreamExt},
    sync::watch,
};

use crate::{
    communication::RecvEndpoint,
    dataflow::{
        operator::{Operator, OperatorConfigT},
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
    /// The configuration with which the operator was instantiated.
    config: Box<dyn OperatorConfigT>,
    /// A merged stream of all the input streams of the operator. This is used to retrieve events
    /// to execute. 
    event_stream: Option<Pin<Box<dyn Send + Stream<Item = Vec<OperatorEvent>>>>>,
    /// Used to decide whether to run destroy()
    streams_closed: HashMap<StreamId, Arc<AtomicBool>>,
    /// A lattice that keeps a partial order of the events that need to be processed.
    lattice: Arc<ExecutionLattice>,
    logger: slog::Logger,
}

impl OperatorExecutor {
    /// Creates a new OperatorEvent.
    pub fn new<T: 'static + Operator, U: 'static + OperatorConfigT>(
        operator: T,
        config: U,
        mut operator_streams: Vec<Box<dyn OperatorExecutorStreamT>>,
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
            config: Box::new(config),
            event_stream,
            streams_closed,
            lattice: Arc::new(ExecutionLattice::new()),
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

    /// A high-level execute function that retrieves events from the input streams, adds them to
    /// the lattice being maintained by the executor and notifies the `event_runner` invocations to
    /// process the received events.
    pub async fn execute(&mut self) {
        if let Some(mut event_stream) = self.event_stream.take() {
            // Launch consumers
            // TODO: use better sync PQ and CondVar instead of watch
            // TODO: launch more consumers and adjust amount based on size of event queue
            let (notifier_tx, notifier_rx) = watch::channel(EventRunnerMessage::AddedEvents);
            let event_runner_fut = Self::event_runner(Arc::clone(&self.lattice), notifier_rx);
            let event_runner_handle = tokio::spawn(event_runner_fut);
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
            event_runner_handle.await.unwrap();

            if self.all_streams_closed() {
                slog::debug!(
                    self.logger,
                    "Destroying operator with name {:?} and ID {}.",
                    self.config.name(),
                    self.config.id(),
                );
                self.operator.destroy();
            }
        }
    }

    // TODO: run multiple of these in parallel?
    /// An `event_runner` invocation is in charge of executing callbacks associated with an event.
    /// Upon receipt of an `AddedEvents` notification, it queries the lattice for events that are
    /// ready to run, executes them, and notifies the lattice of their completion.
    async fn event_runner(
        lattice: Arc<ExecutionLattice>,
        mut notifier_rx: watch::Receiver<EventRunnerMessage>,
    ) {
        // Wait for notification for events added.
        while let Some(control_msg) = notifier_rx.recv().await {
            loop {
                if let Some((event, event_id)) = lattice.get_event().await {
                    (event.callback)();
                    lattice.mark_as_completed(event_id).await;
                } else {
                    break;
                }
            }

            if EventRunnerMessage::DestroyOperator == control_msg {
                break;
            }
        }
    }
}

unsafe impl Send for OperatorExecutor {}
