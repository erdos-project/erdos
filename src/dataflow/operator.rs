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
    pub name: Option<String>,
    /// A unique identifier for every operator.
    pub id: OperatorId,
    pub arg: Option<T>,
    pub flow_watermarks: bool,
    pub node_id: NodeId,
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

    pub fn name(&mut self, name: &str) -> &mut Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn arg(&mut self, arg: T) -> &mut Self {
        self.arg = Some(arg);
        self
    }

    /// `flow_watermarks` is true by default.
    pub fn flow_watermarks(&mut self, flow_watermarks: bool) -> &mut Self {
        self.flow_watermarks = flow_watermarks;
        self
    }

    /// `node_id` is 0 by default.
    // TODO: replace this with scheduling constraints.
    pub fn node(&mut self, node_id: NodeId) -> &mut Self {
        self.node_id = node_id;
        self
    }

    /// Sets the maximum number of callbacks the operator can process in parallel
    /// at a time. Defaults to 1.
    pub fn num_event_runners(&mut self, num_event_runners: usize) -> &mut Self {
        assert!(
            num_event_runners > 0,
            "Operator must have at least 1 thread."
        );
        self.num_event_runners = num_event_runners;
        self
    }

    /// Loses argument type information in the
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
