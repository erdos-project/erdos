use std::{cell::RefCell, rc::Rc};

use serde::Deserialize;

use crate::dataflow::{Data, Message, State, Timestamp};

use super::{
    IngestStream, InternalReadStream, LoopStream, StatefulReadStream, StreamId, WriteStream,
};

#[derive(Clone, Default)]
pub struct ReadStream<D: Data> {
    /// Stores information and internal information about the stream
    internal_stream: Rc<RefCell<InternalReadStream<D>>>,
}

impl<D: Data> ReadStream<D> {
    /// Create a stream into which we can write data.
    pub fn new() -> Self {
        Self {
            internal_stream: Rc::new(RefCell::new(InternalReadStream::new())),
        }
    }

    /// Add a callback to be invoked when the stream receives a message.
    pub fn add_callback<F: 'static + Fn(Timestamp, D)>(&self, callback: F) {
        self.internal_stream.borrow_mut().add_callback(callback);
    }

    /// Add a callback to be invoked after the stream received, and the operator
    /// processed all the messages with a timestamp.
    pub fn add_watermark_callback<F: 'static + Fn(&Timestamp)>(&self, callback: F) {
        self.internal_stream
            .borrow_mut()
            .add_watermark_callback(callback);
    }

    /// Returns a new instance of the stream with state associated to it.
    pub fn add_state<S: State>(&self, state: S) -> StatefulReadStream<D, S> {
        StatefulReadStream::from(self.internal_stream.borrow_mut().add_state(state))
    }

    pub fn get_id(&self) -> StreamId {
        self.internal_stream.borrow().get_id()
    }

    pub fn get_name(&self) -> String {
        self.internal_stream.borrow().get_name().to_string()
    }

    /// Tries to read a message from a channel.
    ///
    /// Returns an immutable reference, or `None` if no messages are
    /// available at the moment (i.e., non-blocking read).
    pub fn try_read(&self) -> Option<Message<D>> {
        self.internal_stream.borrow_mut().try_read()
    }

    /// Blocking read. Returns `None` if the stream doesn't have a receive endpoint.
    pub fn read(&self) -> Option<Message<D>> {
        self.internal_stream.borrow_mut().read()
    }
}

impl<D: Data> From<&ReadStream<D>> for ReadStream<D> {
    fn from(read_stream: &ReadStream<D>) -> Self {
        read_stream.clone()
    }
}

impl<D: Data> From<&WriteStream<D>> for ReadStream<D> {
    fn from(write_stream: &WriteStream<D>) -> Self {
        Self::from(InternalReadStream::new_with_id_name(
            write_stream.get_id(),
            write_stream.get_name(),
        ))
    }
}

impl<D> From<&LoopStream<D>> for ReadStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn from(loop_stream: &LoopStream<D>) -> Self {
        Self::from(InternalReadStream::new_with_id_name(
            loop_stream.get_id(),
            loop_stream.get_name(),
        ))
    }
}

impl<D> From<&IngestStream<D>> for ReadStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn from(ingest_stream: &IngestStream<D>) -> Self {
        Self::from(InternalReadStream::new_with_id_name(
            ingest_stream.get_id(),
            ingest_stream.get_name(),
        ))
    }
}

impl<D: Data> From<InternalReadStream<D>> for ReadStream<D> {
    fn from(internal_stream: InternalReadStream<D>) -> Self {
        Self {
            internal_stream: Rc::new(RefCell::new(internal_stream)),
        }
    }
}

impl<D: Data> From<Rc<RefCell<InternalReadStream<D>>>> for ReadStream<D> {
    fn from(internal_stream: Rc<RefCell<InternalReadStream<D>>>) -> Self {
        Self { internal_stream }
    }
}

impl<D: Data> From<&ReadStream<D>> for Rc<RefCell<InternalReadStream<D>>> {
    fn from(read_stream: &ReadStream<D>) -> Self {
        Rc::clone(&read_stream.internal_stream)
    }
}
