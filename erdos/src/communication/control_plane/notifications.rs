use serde::{Deserialize, Serialize};

use crate::{Uuid, OperatorId};

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
    Initialized(WorkerId),
    OperatorReady(OperatorId),
}

/// A [`LeaderNotification`] specifies the notifications that a
/// [`LeaderNode`](crate::node::LeaderNode) can send to a
/// [`WorkerNode`](crate::node::WorkerNode).
#[derive(Debug, Serialize, Deserialize)]
pub enum LeaderNotification {
    Operator(OperatorId),
    Shutdown,
}