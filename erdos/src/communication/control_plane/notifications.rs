use serde::{Deserialize, Serialize};

use crate::{OperatorId, Uuid, node::Resources};

#[derive(Debug, Clone)]
pub enum DriverNotification {
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
