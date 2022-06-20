use serde::{Deserialize, Serialize};

use crate::{dataflow::graph::{InternalGraph, JobGraph}, node::Resources, OperatorId};

#[derive(Debug, Clone)]
pub enum DriverNotification {
    SubmitGraph(JobGraph),
    Shutdown,
}

/// A [`WorkerNotification`] specifies the notifications that a
/// [`WorkerNode`](crate::node::WorkerNode) can send to a
/// [`LeaderNode`](crate::node::LeaderNode).
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerNotification {
    Initialized(usize, Resources),
    OperatorReady(OperatorId),
    SubmitGraph(InternalGraph),
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
