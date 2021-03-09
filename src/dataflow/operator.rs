use crate::{
    dataflow::{Data, ReadStream, State, Timestamp, WriteStream},
    node::NodeId,
    OperatorId,
};

// /// Trait that must be implemented by any operator.
// pub trait Operator {
//     /// Implement this method if you want to take control of the execution loop of an
//     /// operator (e.g., pull messages from streams).
//     /// Note: No callbacks are invoked before the completion of this method.
//     fn run(&mut self) {}

//     /// Implement this method if you need to do clean-up before the operator completes.
//     /// An operator completes after it has received top watermark on all its read streams.
//     fn destroy(&mut self) {}
// }

pub(crate) trait Operator<S: State, T: Data, U: Data, V: Data, W: Data> {}

/*****************************************************************************
 * Source: sends data with type T                                            *
 *****************************************************************************/

pub trait Source<S: State, T: Data> {
    fn connect() -> WriteStream<T> {
        WriteStream::new()
    }

    fn run(&mut self) {}

    fn destroy(&mut self) {}
}

impl<O, S, T> Operator<S, (), (), T, ()> for O
where
    O: Source<S, T>,
    S: State,
    T: Data,
{
}

/*****************************************************************************
 * Sink: receives data with type T                                           *
 *****************************************************************************/

pub trait Sink<S: State, T: Data> {
    fn connect(read_stream: &ReadStream<T>) {}

    fn run(&mut self) {}

    fn destroy(&mut self) {}

    fn on_data(ctx: &mut SinkContext, data: T);

    fn on_data_stateful(ctx: &mut StatefulSinkContext<S>, data: T);

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

pub trait OneInOneOut<S: State, T: Data, U: Data> {
    fn connect(read_stream: &ReadStream<T>) -> WriteStream<U> {
        WriteStream::new()
    }

    fn run(&mut self) {}

    fn destroy(&mut self) {}

    fn on_data(ctx: &mut OneInOneOutContext<U>, data: T);

    fn on_data_stateful(ctx: &mut StatefulOneInOneOutContext<S, U>, data: T);

    fn on_watermark(ctx: &mut StatefulOneInOneOutContext<S, U>);
}

pub struct OneInOneOutContext<U: Data> {
    pub timestamp: Timestamp,
    pub write_stream: WriteStream<U>,
}

pub struct StatefulOneInOneOutContext<S: State, U: Data> {
    pub timestamp: Timestamp,
    pub write_stream: WriteStream<U>,
    pub state: S,
}

/*****************************************************************************
 * TwoInOneOut: receives T, receives U, sends V                              *
 *****************************************************************************/

pub trait TwoInOneOut<S: State, T: Data, U: Data, V: Data> {
    fn connect(left_stream: &ReadStream<T>, right_stream: &ReadStream<U>) -> WriteStream<V> {
        WriteStream::new()
    }

    fn run(&mut self) {}

    fn destroy(&mut self) {}

    fn on_left_data(ctx: &mut TwoInOneOutContext<U>, data: T);

    fn on_left_data_stateful(ctx: &mut StatefulTwoInOneOutContext<S, V>, data: T);

    fn on_right_data(ctx: &mut TwoInOneOutContext<U>, data: U);

    fn on_right_data_stateful(ctx: &mut StatefulTwoInOneOutContext<S, V>, data: U);

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

pub trait OneInTwoOut<S: State, T: Data, U: Data, V: Data> {
    fn connect(read_stream: &ReadStream<T>) -> (WriteStream<U>, WriteStream<V>) {
        (WriteStream::new(), WriteStream::new())
    }

    fn run(&mut self) {}

    fn destroy(&mut self) {}

    fn on_data(ctx: &mut OneInOneOutContext<U>, data: T);

    fn on_data_stateful(ctx: &mut StatefulOneInTwoOutContext<S, U, V>, data: T);

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
pub struct OperatorConfig<T: Clone> {
    /// A human-readable name for the [`Operator`] used in logging.
    pub name: Option<String>,
    /// A unique identifier for the [`Operator`].
    /// ERDOS sets this value when the dataflow graph executes.
    /// Currently the ID is inaccessible from the driver, but is set when the config
    /// is passed to the operator.
    pub id: OperatorId,
    /// A generically typed argument to the [`Operator`].
    pub arg: Option<T>,
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

impl<T: Clone> OperatorConfig<T> {
    pub fn new() -> Self {
        Self {
            id: OperatorId::nil(),
            name: None,
            arg: None,
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

    /// Set an argument to be passed to the [`Operator`].
    pub fn arg(mut self, arg: T) -> Self {
        self.arg = Some(arg);
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

    /// Removes the argument to lose type information. Used in
    /// [`OperatorExecutor`](crate::node::operator_executor::OperatorExecutor).
    pub(crate) fn drop_arg(self) -> OperatorConfig<()> {
        OperatorConfig {
            id: self.id,
            name: self.name,
            arg: None,
            flow_watermarks: self.flow_watermarks,
            node_id: self.node_id,
            num_event_runners: self.num_event_runners,
        }
    }
}
