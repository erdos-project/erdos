use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::rc::Rc;

use crate::dataflow::{stream::InternalReadStream, Data, EventMakerT, ReadStream, ReadStreamT};
use crate::node::operator_event::OperatorEvent;

/// The internal OperatorExecutor's view of a stream
pub trait OperatorExecutorStreamT {
    fn try_read_events(&mut self) -> Option<Vec<OperatorEvent>>;
}

pub struct OperatorExecutorStream<D: Data> {
    stream: Rc<RefCell<InternalReadStream<D>>>,
}

impl<D: Data> OperatorExecutorStreamT for OperatorExecutorStream<D> {
    fn try_read_events(&mut self) -> Option<Vec<OperatorEvent>> {
        let mut stream = self.stream.borrow_mut();
        match stream.try_read() {
            Some(msg) => Some(stream.make_events(msg)),
            None => None,
        }
    }
}

impl<D: Data> From<Rc<RefCell<InternalReadStream<D>>>> for OperatorExecutorStream<D> {
    fn from(stream: Rc<RefCell<InternalReadStream<D>>>) -> Self {
        Self { stream }
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
        Self { stream }
    }
}

#[allow(dead_code)]
pub struct OperatorExecutor {
    operator_streams: Vec<Box<dyn OperatorExecutorStreamT>>,
    // Priority queue that sorts events by priority and timestamp.
    event_queue: BinaryHeap<OperatorEvent>,
    logger: slog::Logger,
}

impl OperatorExecutor {
    pub fn new(
        operator_streams: Vec<Box<dyn OperatorExecutorStreamT>>,
        logger: slog::Logger,
    ) -> Self {
        Self {
            operator_streams,
            event_queue: BinaryHeap::new(),
            logger,
        }
    }

    pub fn execute(&mut self) {
        loop {
            // Get new events
            for op_stream in self.operator_streams.iter_mut() {
                match op_stream.try_read_events() {
                    Some(events) => self.event_queue.extend(events),
                    None => {}
                };
            }
            // Process next event
            match self.event_queue.pop() {
                Some(event) => (event.callback)(),
                None => {}
            };
        }
    }
}
