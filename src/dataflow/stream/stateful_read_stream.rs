use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::{
    dataflow::{
        callback_builder::{OneReadOneWrite, TwoReadZeroWrite},
        Data, State, Timestamp,
    },
    Uuid,
};

use super::{InternalStatefulReadStream, ReadStream, StreamId, WriteStream};

/// Stream with an associated state.
pub struct StatefulReadStream<D: Data, T: State> {
    /// Stores information and internal information about the stream
    internal_stream: Rc<RefCell<InternalStatefulReadStream<D, T>>>,
}

impl<D: Data, T: State> StatefulReadStream<D, T> {
    /// Createa a new stream with state
    pub fn new(read_stream: &ReadStream<D>, state: T) -> Self {
        read_stream.add_state(state)
    }

    /// Add a callback to be invoked when the stream receives a message.
    /// The callback will be invoked for each message, and will receive the
    /// message and the stream's state as arguments.
    pub fn add_callback<F: 'static + Fn(&Timestamp, &D, &mut T)>(&self, callback: F) {
        self.internal_stream.borrow_mut().add_callback(callback);
    }

    /// Add a callback to be invoked after the stream received, and the operator
    /// processed all the messages with a timestamp.
    pub fn add_watermark_callback<F: 'static + Fn(&Timestamp, &mut T)>(&self, callback: F) {
        self.internal_stream
            .borrow_mut()
            .add_watermark_callback(callback);
    }

    /// Gets a reference to the stream state.
    pub fn get_state(&self) -> Arc<T> {
        self.internal_stream.borrow_mut().get_state()
    }

    pub(crate) fn get_state_id(&self) -> Uuid {
        self.internal_stream.borrow().get_state_id()
    }

    /// Extends the stream into a bundle of two stateful streams.
    pub fn add_read_stream<D1: Data, S1: State>(
        &self,
        read_stream: &StatefulReadStream<D1, S1>,
    ) -> Rc<RefCell<TwoReadZeroWrite<T, S1>>> {
        let builder = Rc::new(RefCell::new(TwoReadZeroWrite::new(self, read_stream)));
        self.internal_stream
            .borrow_mut()
            .add_child(Rc::clone(&builder));
        read_stream
            .internal_stream
            .borrow_mut()
            .add_child(Rc::clone(&builder));
        builder
    }

    /// Extends the stream into a bundle of a read and write stream.
    pub fn add_write_stream<W: Data>(
        &self,
        write_stream: &WriteStream<W>,
    ) -> Rc<RefCell<OneReadOneWrite<T, W>>> {
        let builder = Rc::new(RefCell::new(OneReadOneWrite::new(
            self,
            write_stream.clone(),
        )));
        self.internal_stream
            .borrow_mut()
            .add_child(Rc::clone(&builder));
        builder
    }

    pub fn get_id(&self) -> StreamId {
        self.internal_stream.borrow().get_id()
    }
}

impl<D: Data, S: State> From<Rc<RefCell<InternalStatefulReadStream<D, S>>>>
    for StatefulReadStream<D, S>
{
    fn from(internal_stream: Rc<RefCell<InternalStatefulReadStream<D, S>>>) -> Self {
        Self { internal_stream }
    }
}

impl<D: Data, S: State> From<&StatefulReadStream<D, S>>
    for Rc<RefCell<InternalStatefulReadStream<D, S>>>
{
    fn from(stateful_read_stream: &StatefulReadStream<D, S>) -> Self {
        Rc::clone(&stateful_read_stream.internal_stream)
    }
}
