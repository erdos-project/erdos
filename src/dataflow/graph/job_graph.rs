use petgraph::stable_graph::StableGraph;

use crate::{node::NodeId, OperatorId};

pub enum Endpoint {
    Operator(OperatorId),
    Driver(NodeId),
}

struct Channel {
    stream: StreamId,
}

pub struct JobGraph {
    graph: StableGraph<Endpoint, Channel>,
}
