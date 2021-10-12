use std::sync::{Arc, Mutex};

use serde::Deserialize;

use crate::{
    dataflow::{graph::default_graph, operator::*, AppendableStateT, Data, State, StateT, Stream},
    node::operator_executors::{
        OneInExecutor, OneInOneOutMessageProcessor, OneInTwoOutMessageProcessor, OperatorExecutorT,
        ParallelOneInOneOutMessageProcessor, ParallelOneInTwoOutMessageProcessor,
        ParallelSinkMessageProcessor, ParallelTwoInOneOutMessageProcessor, SinkMessageProcessor,
        SourceExecutor, TwoInExecutor, TwoInOneOutMessageProcessor,
    },
    scheduler::channel_manager::ChannelManager,
    OperatorId,
};

/// Adds a [`Source`] operator, which has no read streams, but introduces data into the dataflow
/// graph by interacting with external data sources (e.g., other systems, sensor data).
pub fn connect_source<O, S, T>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
) -> Stream<T>
where
    O: 'static + Source<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    config.id = OperatorId::new_deterministic();
    let write_stream = Stream::new();

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

    default_graph::add_operator::<_, (), (), T, ()>(
        config.clone(),
        op_runner,
        None,
        None,
        Some(&write_stream),
        None,
    );

    write_stream
}

/// Adds a [`ParallelSink`] operator, which receives data on input read streams and directly
/// interacts with external systems.
pub fn connect_parallel_sink<O, S, T, U>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: impl Into<Stream<T>>,
) where
    O: 'static + ParallelSink<S, T, U>,
    S: AppendableStateT<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
{
    config.id = OperatorId::new_deterministic();

    let read_stream: Stream<T> = read_stream.into();
    let read_stream_ids = vec![read_stream.id()];

    let config_copy = config.clone();
    let read_stream_ids_copy = read_stream_ids.clone();

    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();

            Box::new(OneInExecutor::new(
                config_copy.clone(),
                Box::new(ParallelSinkMessageProcessor::new(
                    config_copy.clone(),
                    operator_fn.clone(),
                    state_fn.clone(),
                )),
                read_stream,
            ))
        };

    default_graph::add_operator::<_, T, (), (), ()>(
        config,
        op_runner,
        Some(&read_stream),
        None,
        None,
        None,
    );
}

/// Adds a [`Sink`] operator, which receives data on input read streams and directly interacts
/// with external systems.
pub fn connect_sink<O, S, T>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: impl Into<Stream<T>>,
) where
    O: 'static + Sink<S, T>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
{
    config.id = OperatorId::new_deterministic();

    let read_stream: Stream<T> = read_stream.into();
    let read_stream_ids = vec![read_stream.id()];

    let config_copy = config.clone();
    let read_stream_ids_copy = read_stream_ids.clone();

    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();

            Box::new(OneInExecutor::new(
                config_copy.clone(),
                Box::new(SinkMessageProcessor::new(
                    config_copy.clone(),
                    operator_fn.clone(),
                    state_fn.clone(),
                )),
                read_stream,
            ))
        };

    default_graph::add_operator::<_, T, (), (), ()>(
        config.clone(),
        op_runner,
        Some(&read_stream),
        None,
        None,
        None,
    );
}

/// Adds a [`ParallelOneInOneOut`] operator that has one input read stream and one output
/// write stream.
pub fn connect_parallel_one_in_one_out<O, S, T, U, V>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: impl Into<Stream<T>>,
) -> Stream<U>
where
    O: 'static + ParallelOneInOneOut<S, T, U, V>,
    S: AppendableStateT<V>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: 'static + Send + Sync,
{
    config.id = OperatorId::new_deterministic();

    let read_stream: Stream<T> = read_stream.into();
    let read_stream_ids = vec![read_stream.id()];

    let write_stream = Stream::new();
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

            Box::new(OneInExecutor::new(
                config_copy.clone(),
                Box::new(ParallelOneInOneOutMessageProcessor::new(
                    config_copy.clone(),
                    operator_fn.clone(),
                    state_fn.clone(),
                    write_stream,
                )),
                read_stream,
            ))
        };

    default_graph::add_operator::<_, T, (), U, ()>(
        config.clone(),
        op_runner,
        Some(&read_stream),
        None,
        Some(&write_stream),
        None,
    );

    write_stream
}

/// Adds a [`OneInOneOut`] operator that has one input read stream and one output write stream.
pub fn connect_one_in_one_out<O, S, T, U>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: impl Into<Stream<T>>,
) -> Stream<U>
where
    O: 'static + OneInOneOut<S, T, U>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    config.id = OperatorId::new_deterministic();

    let read_stream: Stream<T> = read_stream.into();
    let read_stream_ids = vec![read_stream.id()];

    let write_stream = Stream::new();
    let write_stream_ids = vec![write_stream.id()];

    let config_copy = config.clone();
    let read_stream_ids_copy = read_stream_ids.clone();
    let write_stream_ids_copy = write_stream_ids.clone();

    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();
            let write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();

            Box::new(OneInExecutor::new(
                config_copy.clone(),
                Box::new(OneInOneOutMessageProcessor::new(
                    config_copy.clone(),
                    operator_fn.clone(),
                    state_fn.clone(),
                    write_stream,
                )),
                read_stream,
            ))
        };

    default_graph::add_operator::<_, T, (), U, ()>(
        config.clone(),
        op_runner,
        Some(&read_stream),
        None,
        Some(&write_stream),
        None,
    );

    write_stream
}

/// Adds a [`ParallelTwoInOneOut`] operator that has two input read streams and one output
/// write stream.
pub fn connect_parallel_two_in_one_out<O, S, T, U, V, W>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    left_read_stream: impl Into<Stream<T>>,
    right_read_stream: impl Into<Stream<U>>,
) -> Stream<V>
where
    O: 'static + ParallelTwoInOneOut<S, T, U, V, W>,
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
    W: 'static + Send + Sync,
{
    config.id = OperatorId::new_deterministic();

    let left_read_stream: Stream<T> = left_read_stream.into();
    let right_read_stream: Stream<U> = right_read_stream.into();
    let read_stream_ids = vec![left_read_stream.id(), right_read_stream.id()];

    let write_stream = Stream::new();
    let write_stream_ids = vec![write_stream.id()];

    let config_copy = config.clone();
    let read_stream_ids_copy = read_stream_ids.clone();
    let write_stream_ids_copy = write_stream_ids.clone();

    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let left_read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();
            let right_read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[1])
                .unwrap();
            let write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();

            Box::new(TwoInExecutor::new(
                config_copy.clone(),
                Box::new(ParallelTwoInOneOutMessageProcessor::new(
                    config_copy.clone(),
                    operator_fn.clone(),
                    state_fn.clone(),
                    write_stream,
                )),
                left_read_stream,
                right_read_stream,
            ))
        };

    default_graph::add_operator::<_, T, U, V, ()>(
        config.clone(),
        op_runner,
        Some(&left_read_stream),
        Some(&right_read_stream),
        Some(&write_stream),
        None,
    );

    write_stream
}

/// Adds a [`TwoInOneOut`] operator that has two input read streams and one output write stream.
pub fn connect_two_in_one_out<O, S, T, U, V>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    left_read_stream: impl Into<Stream<T>>,
    right_read_stream: impl Into<Stream<U>>,
) -> Stream<V>
where
    O: 'static + TwoInOneOut<S, T, U, V>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    config.id = OperatorId::new_deterministic();

    let left_read_stream: Stream<T> = left_read_stream.into();
    let right_read_stream: Stream<U> = right_read_stream.into();
    let read_stream_ids = vec![left_read_stream.id(), right_read_stream.id()];

    let write_stream = Stream::new();
    let write_stream_ids = vec![write_stream.id()];

    let read_stream_ids_copy = read_stream_ids.clone();
    let write_stream_ids_copy = write_stream_ids.clone();
    let config_copy = config.clone();
    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let left_read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();
            let right_read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[1])
                .unwrap();
            let write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();

            Box::new(TwoInExecutor::new(
                config_copy.clone(),
                Box::new(TwoInOneOutMessageProcessor::new(
                    config_copy.clone(),
                    operator_fn.clone(),
                    state_fn.clone(),
                    write_stream,
                )),
                left_read_stream,
                right_read_stream,
            ))
        };

    default_graph::add_operator::<_, T, U, V, ()>(
        config.clone(),
        op_runner,
        Some(&left_read_stream),
        Some(&right_read_stream),
        Some(&write_stream),
        None,
    );

    write_stream
}

/// Adds a [`ParallelOneInTwoOut`] operator that has one input read stream and two output
/// write streams.
pub fn connect_parallel_one_in_two_out<O, S, T, U, V, W>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: impl Into<Stream<T>>,
) -> (Stream<U>, Stream<V>)
where
    O: 'static + ParallelOneInTwoOut<S, T, U, V, W>,
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
    W: 'static + Send + Sync,
{
    config.id = OperatorId::new_deterministic();

    let read_stream: Stream<T> = read_stream.into();
    let read_stream_ids = vec![read_stream.id()];

    let left_write_stream = Stream::new();
    let right_write_stream = Stream::new();
    let write_stream_ids = vec![left_write_stream.id(), right_write_stream.id()];

    let read_stream_ids_copy = read_stream_ids.clone();
    let write_stream_ids_copy = write_stream_ids.clone();
    let config_copy = config.clone();
    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();
            let left_write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();
            let right_write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();

            Box::new(OneInExecutor::new(
                config_copy.clone(),
                Box::new(ParallelOneInTwoOutMessageProcessor::new(
                    config_copy.clone(),
                    operator_fn.clone(),
                    state_fn.clone(),
                    left_write_stream,
                    right_write_stream,
                )),
                read_stream,
            ))
        };

    default_graph::add_operator::<_, T, (), U, V>(
        config.clone(),
        op_runner,
        Some(&read_stream),
        None,
        Some(&left_write_stream),
        Some(&right_write_stream),
    );

    (left_write_stream, right_write_stream)
}

/// Adds a [`OneInTwoOut`] operator that has one input read stream and two output write streams.
pub fn connect_one_in_two_out<O, S, T, U, V>(
    operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
    state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
    mut config: OperatorConfig,
    read_stream: impl Into<Stream<T>>,
) -> (Stream<U>, Stream<V>)
where
    O: 'static + OneInTwoOut<S, T, U, V>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    config.id = OperatorId::new_deterministic();

    let read_stream: Stream<T> = read_stream.into();
    let read_stream_ids = vec![read_stream.id()];

    let left_write_stream = Stream::new();
    let right_write_stream = Stream::new();
    let write_stream_ids = vec![left_write_stream.id(), right_write_stream.id()];

    let read_stream_ids_copy = read_stream_ids.clone();
    let write_stream_ids_copy = write_stream_ids.clone();
    let config_copy = config.clone();
    let op_runner =
        move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
            let mut channel_manager = channel_manager.lock().unwrap();

            let read_stream = channel_manager
                .take_read_stream(read_stream_ids_copy[0])
                .unwrap();
            let left_write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();
            let right_write_stream = channel_manager
                .get_write_stream(write_stream_ids_copy[0])
                .unwrap();

            Box::new(OneInExecutor::new(
                config_copy.clone(),
                Box::new(OneInTwoOutMessageProcessor::new(
                    config_copy.clone(),
                    operator_fn.clone(),
                    state_fn.clone(),
                    left_write_stream,
                    right_write_stream,
                )),
                read_stream,
            ))
        };

    default_graph::add_operator::<_, T, (), U, V>(
        config.clone(),
        op_runner,
        Some(&read_stream),
        None,
        Some(&left_write_stream),
        Some(&right_write_stream),
    );

    (left_write_stream, right_write_stream)
}
