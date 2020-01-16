use crate::{node::NodeId, OperatorId};

/// Trait that must be implemented by any operator.
pub trait OperatorT {
    /// Gets the id of the operator.
    fn get_id(&self) -> OperatorId;

    /// Gets the string name of the operator.
    fn get_name(&self) -> String;

    /// Implement this method if you want to take control of the execution loop of an
    /// operator (e.g., pull messages from streams).
    /// Note: No callbacks are invoked before the completion of this method.
    fn run(&self) {}

    /// Implement this method if you need to do clean-up before the operator completes.
    /// An operator completes after it has received the top watermark on all its read streams.
    fn destroy(&self) {}
}

#[derive(Clone)]
pub struct OperatorConfig<T: Clone> {
    pub name: String,
    /// A unique identifier for every operator.
    pub id: OperatorId,
    pub arg: T,
    pub flow_watermarks: bool,
    pub node_id: NodeId,
}

impl<T: Clone> OperatorConfig<T> {
    pub fn new(name: &str, arg: T, flow_watermarks: bool, node_id: NodeId) -> Self {
        Self {
            name: name.to_string(),
            id: OperatorId::nil(),
            arg,
            flow_watermarks,
            node_id,
        }
    }
}

impl<T: Clone> From<T> for OperatorConfig<T> {
    fn from(arg: T) -> Self {
        Self::new("", arg, true, 0)
    }
}
