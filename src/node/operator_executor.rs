use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use futures::future;
use serde::Deserialize;
use tokio::{
    self,
    stream::{Stream, StreamExt},
    sync::{mpsc, watch},
};

use crate::{
    communication::{ControlMessage, RecvEndpoint},
    dataflow::{
        operator::{
            OneInOneOut, OneInOneOutContext, OneInTwoOut, OneInTwoOutContext, OperatorConfig, Sink,
            SinkContext, Source, TwoInOneOut, TwoInOneOutContext,
        },
        stream::{InternalReadStream, StreamId},
        Data, EventMakerT, Message, ReadStream, State, WriteStream,
    },
    node::lattice::ExecutionLattice,
    node::operator_event::OperatorEvent,
    scheduler::channel_manager::ChannelManager,
};

// TODO: use indirection to make this private.
pub trait OperatorExecutorT: Send {
    // Returns a future for OperatorExecutor::execute().
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>>;
}

pub struct SourceExecutor<O, S, T>
where
    O: Source<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    operator: O,
    state: S,
    write_stream: WriteStream<T>,
}

impl<O, S, T> SourceExecutor<O, S, T>
where
    O: Source<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        write_stream: WriteStream<T>,
    ) -> Self {
        Self {
            operator: operator_fn(),
            state: state_fn(),
            write_stream,
        }
    }

    pub async fn execute(&mut self) {
        unimplemented!()
    }
}

impl<O, S, T> OperatorExecutorT for SourceExecutor<O, S, T>
where
    O: Source<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
    }
}

pub struct SinkExecutor<O, S, T>
where
    O: Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    operator: O,
    state: S,
    read_stream: ReadStream<T>,
}

impl<O, S, T> SinkExecutor<O, S, T>
where
    O: Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        read_stream: ReadStream<T>,
    ) -> Self {
        Self {
            operator: operator_fn(),
            state: state_fn(),
            read_stream,
        }
    }

    pub async fn execute(&mut self) {
        unimplemented!()
    }
}

impl<O, S, T> OperatorExecutorT for SinkExecutor<O, S, T>
where
    O: Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
    }
}

pub struct OneInOneOutExecutor<O, S, T, U>
where
    O: OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    operator: O,
    state: S,
    read_stream: ReadStream<T>,
    write_stream: WriteStream<U>,
}

impl<O, S, T, U> OneInOneOutExecutor<O, S, T, U>
where
    O: OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        read_stream: ReadStream<T>,
        write_stream: WriteStream<U>,
    ) -> Self {
        Self {
            operator: operator_fn(),
            state: state_fn(),
            read_stream,
            write_stream,
        }
    }

    pub async fn execute(&mut self) {
        unimplemented!()
    }
}

impl<O, S, T, U> OperatorExecutorT for OneInOneOutExecutor<O, S, T, U>
where
    O: OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
    }
}

pub struct TwoInOneOutExecutor<O, S, T, U, V>
where
    O: TwoInOneOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    operator: O,
    state: S,
    left_read_stream: ReadStream<T>,
    right_read_stream: ReadStream<U>,
    write_stream: WriteStream<V>,
}

impl<O, S, T, U, V> TwoInOneOutExecutor<O, S, T, U, V>
where
    O: TwoInOneOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        left_read_stream: ReadStream<T>,
        right_read_stream: ReadStream<U>,
        write_stream: WriteStream<V>,
    ) -> Self {
        Self {
            operator: operator_fn(),
            state: state_fn(),
            left_read_stream,
            right_read_stream,
            write_stream,
        }
    }

    pub async fn execute(&mut self) {
        unimplemented!()
    }
}

impl<O, S, T, U, V> OperatorExecutorT for TwoInOneOutExecutor<O, S, T, U, V>
where
    O: TwoInOneOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
    }
}

pub struct OneInTwoOutExecutor<O, S, T, U, V>
where
    O: OneInTwoOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    operator: O,
    state: S,
    read_stream: ReadStream<T>,
    left_write_stream: WriteStream<U>,
    right_write_stream: WriteStream<V>,
}

impl<O, S, T, U, V> OneInTwoOutExecutor<O, S, T, U, V>
where
    O: OneInTwoOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        read_stream: ReadStream<T>,
        left_write_stream: WriteStream<U>,
        right_write_stream: WriteStream<V>,
    ) -> Self {
        Self {
            operator: operator_fn(),
            state: state_fn(),
            read_stream,
            left_write_stream,
            right_write_stream,
        }
    }

    pub async fn execute(&mut self) {
        unimplemented!()
    }
}

impl<O, S, T, U, V> OperatorExecutorT for OneInTwoOutExecutor<O, S, T, U, V>
where
    O: OneInTwoOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
    }
}
