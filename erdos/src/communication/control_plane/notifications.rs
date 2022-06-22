use std::{collections::HashMap, net::SocketAddr};

use serde::{Deserialize, Serialize};

use crate::{
    dataflow::graph::{InternalGraph, JobGraph},
    node::WorkerState,
    OperatorId,
};

#[derive(Debug, Clone)]
pub(crate) enum DriverNotification {
    RegisterGraph(JobGraph),
    SubmitGraph(String),
    Shutdown,
}

/// A [`WorkerNotification`] specifies the notifications that a
/// [`WorkerNode`](crate::node::WorkerNode) can send to a
/// [`LeaderNode`](crate::node::LeaderNode).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum WorkerNotification {
    Initialized(WorkerState),
    OperatorReady(String, OperatorId),
    SubmitGraph(String, InternalGraph),
    Shutdown,
}

/// A [`LeaderNotification`] specifies the notifications that a
/// [`LeaderNode`](crate::node::LeaderNode) can send to a
/// [`WorkerNode`](crate::node::WorkerNode).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum LeaderNotification {
    ScheduleOperator(String, OperatorId, HashMap<usize, SocketAddr>),
    ExecuteGraph(String),
    Shutdown,
}
