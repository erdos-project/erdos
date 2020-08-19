use crate::{node::NodeId, OperatorId};

/// Trait that must be implemented by any operator.
pub trait Operator {
    /// Implement this method if you want to take control of the execution loop of an
    /// operator (e.g., pull messages from streams).
    /// Note: No callbacks are invoked before the completion of this method.
    fn run(&mut self) {}

    /// Implement this method if you need to do clean-up before the operator completes.
    /// An operator completes after it has received top watermark on all its read streams.
    fn destroy(&mut self) {}
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
