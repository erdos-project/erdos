use serde::{Deserialize, Serialize};

use crate::{dataflow::graph::AbstractGraph, node::Resources, OperatorId, Uuid};

#[derive(Debug, Clone)]
pub enum DriverNotification {
    SubmitGraph(AbstractGraph),
    Shutdown,
}

pub type WorkerId = Uuid;

/// A [`WorkerNotification`] specifies the notifications that a
/// [`WorkerNode`](crate::node::WorkerNode) can send to a
/// [`LeaderNode`](crate::node::LeaderNode).
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerNotification {
    Initialized(WorkerId, Resources),
    OperatorReady(OperatorId),
    SubmitGraph(AbstractGraph),
    Shutdown,
}

/// A [`LeaderNotification`] specifies the notifications that a
/// [`LeaderNode`](crate::node::LeaderNode) can send to a
/// [`WorkerNode`](crate::node::WorkerNode).
#[derive(Debug, Serialize, Deserialize)]
pub enum LeaderNotification {
    ScheduleOperator(OperatorId),
    Shutdown,
}
