use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::rc::Rc;

use async_trait::async_trait;
use tokio;
use tokio::stream::Stream;
use tokio::stream::StreamExt;

use crate::communication::RecvEndpoint;
use crate::dataflow::{stream::InternalReadStream, Data, EventMakerT, Message, ReadStream};
use crate::node::operator_event::OperatorEvent;

use std::pin::Pin;
use std::task::{Context, Poll};

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
    // operator_streams: Vec<Box<dyn OperatorExecutorStreamT>>,
    // Priority queue that sorts events by priority and timestamp.
    event_queue: BinaryHeap<OperatorEvent>,
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
            .map(|x| Box::into_pin(x));
        Self {
            event_stream,
            event_queue: BinaryHeap::new(),
            logger,
        }
    }

    pub async fn execute(&mut self) {
        match self.event_stream.take() {
            Some(mut event_stream) => {
                while let Some(events) = event_stream.next().await {
                    for event in events {
                        (event.callback)()
                    }
                }
            }
            None => (),
        }
    }
}

unsafe impl Send for OperatorExecutor {}
