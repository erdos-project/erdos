use std::sync::{Arc, Mutex};

use futures::channel;
use serde::Deserialize;

use crate::{
    communication::RecvEndpoint,
    dataflow::{
        graph::default_graph, operator::*, Data, Message, ReadStream, State, Timestamp, WriteStream,
    },
    node::{
        operator_executor::{
            OneInOneOutExecutor, OneInTwoOutExecutor, OperatorExecutorT, SinkExecutor,
            SourceExecutor, TwoInOneOutExecutor,
        },
        NodeId,
    },
    scheduler::channel_manager::ChannelManager,
    OperatorId, Uuid,
};

pub fn connect_source<O, S, T>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
) -> WriteStream<T>
where
    O: 'static + Source<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    config.id = OperatorId::new_deterministic();
    let write_stream = WriteStream::new();

    let write_stream_ids = vec![write_stream.id()];

    let write_stream_ids_copy = write_stream_ids.clone();
    let config_copy = config.clone();
    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();

            let executor = SourceExecutor::new(
                config_copy.clone(),
                operator_fn.clone(),
                state_fn.clone(),
                write_stream,
            );

            Box::new(executor)
        };

    default_graph::add_operator(
        config.id,
        config.name,
        config.node_id,
        Vec::new(),
        write_stream_ids,
        op_runner,
    );
    default_graph::add_operator_stream(config.id, &write_stream);

    write_stream
}

pub fn connect_sink<O, S, T>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: &ReadStream<T>,
) where
    O: 'static + Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    config.id = OperatorId::new_deterministic();

    let read_stream_ids = vec![read_stream.id()];

    let read_stream_ids_copy = read_stream_ids.clone();
    let config_copy = config.clone();
    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();

            let executor = SinkExecutor::new(
                config_copy.clone(),
                operator_fn.clone(),
                state_fn.clone(),
                read_stream,
            );

            Box::new(executor)
        };

    default_graph::add_operator(
        config.id,
        config.name,
        config.node_id,
        read_stream_ids,
        vec![],
        op_runner,
    );
}

pub fn connect_one_in_one_out<O, S, T, U>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: &ReadStream<T>,
) -> WriteStream<U>
where
    O: 'static + OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    config.id = OperatorId::new_deterministic();
    let write_stream = WriteStream::new();

    let read_stream_ids = vec![read_stream.id()];
    let write_stream_ids = vec![write_stream.id()];

    let read_stream_ids_copy = read_stream_ids.clone();
    let write_stream_ids_copy = write_stream_ids.clone();
    let config_copy = config.clone();
    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();
            let write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();

            let executor = OneInOneOutExecutor::new(
                config_copy.clone(),
                operator_fn.clone(),
                state_fn.clone(),
                read_stream,
                write_stream,
            );

            Box::new(executor)
        };

    default_graph::add_operator(
        config.id,
        config.name,
        config.node_id,
        read_stream_ids,
        write_stream_ids,
        op_runner,
    );
    default_graph::add_operator_stream(config.id, &write_stream);

    write_stream
}

pub fn connect_two_in_one_out<O, S, T, U, V>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    left_read_stream: &ReadStream<T>,
    right_read_stream: &ReadStream<U>,
) -> WriteStream<V>
where
    O: 'static + TwoInOneOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    unimplemented!()
}

pub fn connect_one_in_two_out<O, S, T, U, V>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: &ReadStream<T>,
) -> (WriteStream<U>, WriteStream<V>)
where
    O: 'static + OneInTwoOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    unimplemented!()
}
