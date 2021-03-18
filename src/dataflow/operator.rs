use std::sync::{Arc, Mutex};

use futures::channel;
use serde::Deserialize;

use crate::{
    communication::RecvEndpoint,
    dataflow::{graph::default_graph, Data, Message, ReadStream, State, Timestamp, WriteStream},
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

/*****************************************************************************
 * Source: sends data with type T                                            *
 *****************************************************************************/

pub trait Source<S, T>: Send
where
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    fn connect<O: 'static + Source<S, T>>(
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
    ) -> WriteStream<T> {
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

    fn run(&mut self, write_stream: &mut WriteStream<T>) {}

    fn destroy(&mut self) {}
}

/*****************************************************************************
 * Sink: receives data with type T                                           *
 *****************************************************************************/

pub trait Sink<S: State, T: Data>: Send {
    fn connect(read_stream: &ReadStream<T>) {}

    fn run(&mut self) {}

    fn destroy(&mut self) {}

    fn on_data(ctx: &mut SinkContext, data: &T);

    fn on_data_stateful(ctx: &mut StatefulSinkContext<S>, data: &T);

    fn on_watermark(ctx: &mut StatefulSinkContext<S>);
}

pub struct SinkContext {
    timestamp: Timestamp,
}

pub struct StatefulSinkContext<S: State> {
    pub timestamp: Timestamp,
    // TODO: change this a managed reference.
    pub state: S,
}

/*****************************************************************************
 * OneInOneOut: receives T, sends U                                          *
 *****************************************************************************/

pub trait OneInOneOut<S, T, U>: Send
where
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    /// The default implementation of this function should not be overridden.
    fn connect<O: 'static + OneInOneOut<S, T, U>>(
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        read_stream: &ReadStream<T>,
    ) -> WriteStream<U> {
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

    fn run(&mut self, read_stream: &mut ReadStream<T>, write_stream: &mut WriteStream<U>) {}

    fn destroy(&mut self) {}

    fn on_data(ctx: &mut OneInOneOutContext<U>, data: &T);

    fn on_data_stateful(ctx: &mut StatefulOneInOneOutContext<S, U>, data: &T);

    fn on_watermark(ctx: &mut StatefulOneInOneOutContext<S, U>);
}

pub struct OneInOneOutContext<U: Data> {
    pub timestamp: Timestamp,
    pub write_stream: WriteStream<U>,
}

pub struct StatefulOneInOneOutContext<S: State, U: Data> {
    pub timestamp: Timestamp,
    pub write_stream: WriteStream<U>,
    // Hacky...
    pub state: Arc<tokio::sync::Mutex<S>>,
}

/*****************************************************************************
 * TwoInOneOut: receives T, receives U, sends V                              *
 *****************************************************************************/

pub trait TwoInOneOut<S: State, T: Data, U: Data, V: Data>: Send {
    fn connect(left_stream: &ReadStream<T>, right_stream: &ReadStream<U>) -> WriteStream<V> {
        WriteStream::new()
    }

    fn run(&mut self) {}

    fn destroy(&mut self) {}

    fn on_left_data(ctx: &mut TwoInOneOutContext<U>, data: &T);

    fn on_left_data_stateful(ctx: &mut StatefulTwoInOneOutContext<S, V>, data: &T);

    fn on_right_data(ctx: &mut TwoInOneOutContext<U>, data: &U);

    fn on_right_data_stateful(ctx: &mut StatefulTwoInOneOutContext<S, V>, data: &U);

    /// Executes when min(left_watermark, right_watermark) advances.
    fn on_watermark(ctx: &mut StatefulTwoInOneOutContext<S, V>);
}

pub struct TwoInOneOutContext<U: Data> {
    pub timestamp: Timestamp,
    pub write_stream: WriteStream<U>,
}

pub struct StatefulTwoInOneOutContext<S: State, U: Data> {
    pub timestamp: Timestamp,
    pub write_stream: WriteStream<U>,
    pub state: S,
}

/*****************************************************************************
 * OneInTwoOut: receives T, sends U, sends V                                 *
 *****************************************************************************/

pub trait OneInTwoOut<S: State, T: Data, U: Data, V: Data>: Send {
    fn connect(read_stream: &ReadStream<T>) -> (WriteStream<U>, WriteStream<V>) {
        (WriteStream::new(), WriteStream::new())
    }

    fn run(&mut self) {}

    fn destroy(&mut self) {}

    fn on_data(ctx: &mut OneInTwoOutContext<U, V>, data: &T);

    fn on_data_stateful(ctx: &mut StatefulOneInTwoOutContext<S, U, V>, data: &T);

    fn on_watermark(ctx: &mut StatefulOneInTwoOutContext<S, U, V>);
}

pub struct OneInTwoOutContext<U: Data, V: Data> {
    pub timestamp: Timestamp,
    pub left_write_stream: WriteStream<U>,
    pub right_write_stream: WriteStream<V>,
}

pub struct StatefulOneInTwoOutContext<S: State, U: Data, V: Data> {
    pub timestamp: Timestamp,
    pub left_write_stream: WriteStream<U>,
    pub right_write_stream: WriteStream<V>,
    pub state: S,
}

#[derive(Clone)]
pub struct OperatorConfig {
    /// A human-readable name for the [`Operator`] used in logging.
    pub name: Option<String>,
    /// A unique identifier for the [`Operator`].
    /// ERDOS sets this value when the dataflow graph executes.
    /// Currently the ID is inaccessible from the driver, but is set when the config
    /// is passed to the operator.
    pub id: OperatorId,
    /// Whether the [`Operator`] should automatically send
    /// [watermark messages](crate::dataflow::Message::Watermark) on all
    /// [`WriteStream`](crate::dataflow::WriteStream)s for
    /// [`Timestamp`](crate::dataflow::Timestamp) `t` upon receiving watermarks with
    /// [`Timestamp`](crate::dataflow::Timestamp) greater than `t` on all
    /// [`ReadStream`](crate::dataflow::ReadStream)s.
    /// Note that watermarks only flow after all watermark callbacks with timestamp
    /// less than `t` complete. Watermarks flow after [`Operator::run`] finishes
    /// running. Defaults to `true`.
    pub flow_watermarks: bool,
    /// The ID of the node on which the operator should run. Defaults to `0`.
    pub node_id: NodeId,
    /// Number of parallel tasks which process callbacks.
    /// A higher number may result in more parallelism; however this may be limited
    /// by dependencies on [`State`](crate::dataflow::State) and timestamps.
    pub num_event_runners: usize,
}

impl OperatorConfig {
    pub fn new() -> Self {
        Self {
            id: OperatorId::nil(),
            name: None,
            flow_watermarks: true,
            node_id: 0,
            num_event_runners: 1,
        }
    }

    /// Set the [`Operator`]'s name.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set whether the [`Operator`] should flow watermarks.
    pub fn flow_watermarks(mut self, flow_watermarks: bool) -> Self {
        self.flow_watermarks = flow_watermarks;
        self
    }

    /// Set the node on which the [`Operator`] runs.
    pub fn node(mut self, node_id: NodeId) -> Self {
        self.node_id = node_id;
        self
    }

    /// Sets the maximum number of callbacks the operator can process in parallel
    /// at a time. Defaults to 1.
    pub fn num_event_runners(mut self, num_event_runners: usize) -> Self {
        assert!(
            num_event_runners > 0,
            "Operator must have at least 1 thread."
        );
        self.num_event_runners = num_event_runners;
        self
    }

    /// Returns the name operator. If the name is not set,
    /// returns the ID of the operator.
    pub fn get_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| format!("{}", self.id))
    }
}
