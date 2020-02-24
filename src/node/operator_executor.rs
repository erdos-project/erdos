use std::{
    cell::RefCell,
    collections::BinaryHeap,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::{
    self,
    stream::{Stream, StreamExt},
    sync::{watch, Mutex},
};

use crate::{
    communication::RecvEndpoint,
    dataflow::{stream::InternalReadStream, Data, EventMakerT, Message, ReadStream},
    node::operator_event::OperatorEvent,
};

pub struct OperatorExecutorStream<D: Data> {
    stream: Rc<RefCell<InternalReadStream<D>>>,
    recv_endpoint: Option<RecvEndpoint<Message<D>>>,
}

impl<D: Data> Stream for OperatorExecutorStream<D> {
    // Item = OperatorEvent might be better?
    type Item = Vec<OperatorEvent>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Vec<OperatorEvent>>> {
        let mut mut_self = self.as_mut();
        if mut_self.recv_endpoint.is_none() {
            let endpoint = mut_self.stream.borrow_mut().take_endpoint();
            mut_self.recv_endpoint = endpoint;
        }
        match mut_self.recv_endpoint.as_mut() {
            Some(RecvEndpoint::InterThread(rx)) => match rx.poll_recv(cx) {
                Poll::Ready(Some(msg)) => Poll::Ready(Some(self.stream.borrow().make_events(msg))),
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
        Self {
            stream,
            recv_endpoint: None,
        }
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
        Self {
            stream,
            recv_endpoint: None,
        }
    }
}

#[allow(dead_code)]
pub struct OperatorExecutor {
    event_stream: Option<Pin<Box<dyn Send + Stream<Item = Vec<OperatorEvent>>>>>,
    // Priority queue that sorts events by priority and timestamp.
    event_queue: Arc<Mutex<BinaryHeap<OperatorEvent>>>,
    logger: slog::Logger,
}

impl OperatorExecutor {
    pub fn new(
        mut operator_streams: Vec<Box<dyn Send + Stream<Item = Vec<OperatorEvent>>>>,
        logger: slog::Logger,
    ) -> Self {
        let event_stream = operator_streams
            .pop()
            .map(|first| {
                operator_streams.into_iter().fold(first, |x, y| {
                    Box::new(StreamExt::merge(Box::into_pin(x), Box::into_pin(y)))
                })
            })
            .map(Box::into_pin);
        Self {
            event_stream,
            event_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            logger,
        }
    }

    pub async fn execute(&mut self) {
        if let Some(mut event_stream) = self.event_stream.take() {
            // Launch consumers
            // TODO: use better sync PQ and CondVar instead of watch
            // TODO: launch more consumers and adjust amount based on size of event queue
            let (notifier_tx, notifier_rx) = watch::channel(());
            let event_consumer_fut = Self::event_runner(Arc::clone(&self.event_queue), notifier_rx);
            tokio::spawn(event_consumer_fut);
            while let Some(events) = event_stream.next().await {
                {
                    {
                        self.event_queue.lock().await.extend(events);
                    }
                    // Notify receivers that new events were added.
                    notifier_tx.broadcast(()).unwrap();
                }
            }
        }
    }

    // TODO: run multiple of these in parallel?
    async fn event_runner(
        event_queue: Arc<Mutex<BinaryHeap<OperatorEvent>>>,
        mut notifier_rx: watch::Receiver<()>,
    ) {
        // Wait for notification for events added.
        while let Some(_) = notifier_rx.recv().await {
            while let Some(event) = { event_queue.lock().await.pop() } {
                (event.callback)();
            }
        }
    }
}

unsafe impl Send for OperatorExecutor {}
