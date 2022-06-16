use serde::{Deserialize, Serialize};

use crate::{dataflow::graph::AbstractGraph, node::Resources, OperatorId, Uuid};

#[derive(Clone)]
pub enum DriverNotification {
    SubmitGraph(AbstractGraph),
    Shutdown,
}

impl std::fmt::Debug for DriverNotification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // TODO (Sukrit): We should assign an ID
            Self::SubmitGraph(arg0) => write!(f, "SubmitGraph(AbstractGraph)"),
            Self::Shutdown => write!(f, "Shutdown"),
        }
    }
}

pub type WorkerId = Uuid;

/// A [`WorkerNotification`] specifies the notifications that a
/// [`WorkerNode`](crate::node::WorkerNode) can send to a
/// [`LeaderNode`](crate::node::LeaderNode).
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerNotification {
    Initialized(WorkerId, Resources),
    OperatorReady(OperatorId),
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
