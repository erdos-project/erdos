use std::{collections::HashMap, net::SocketAddr};

use serde::{Deserialize, Serialize};

use crate::{
    dataflow::graph::{AbstractJobGraph, Job, JobGraph, JobGraphId},
    node::{WorkerId, WorkerState},
};

#[derive(Debug, Clone)]
pub(crate) enum DriverNotification {
    RegisterGraph(JobGraph),
    SubmitGraph(JobGraphId),
    Shutdown,
}

/// A [`WorkerNotification`] specifies the notifications that a
/// [`WorkerNode`](crate::node::WorkerNode) can send to a
/// [`LeaderNode`](crate::node::LeaderNode).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum WorkerNotification {
    /// Informs the Leader that a new Worker has been initialized and provides
    /// information about the ID, Address and Resources of the Worker.
    Initialized(WorkerState),
    /// Informs the Leader that the Job from the graph with the provided ID
    /// is ready for execution.
    JobReady(JobGraphId, Job),
    /// Submits a representation of the JobGraph with the given ID for
    /// scheduling by the Leader.
    SubmitGraph(JobGraphId, AbstractJobGraph),
    /// Informs the Leader that the Worker is shutting down.
    Shutdown,
}

/// A [`WorkerAddress`] enum is used to encapsulate the address of
/// the Worker where a Job is originating from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WorkerAddress {
    /// The [`Job`] originates from a remote address with the given
    /// ID and Socket address.
    Remote(WorkerId, SocketAddr),
    /// The [`Job`] originates on the local Worker.
    Local,
}

/// A [`LeaderNotification`] specifies the notifications that a
/// [`LeaderNode`](crate::node::LeaderNode) can send to a
/// [`WorkerNode`](crate::node::WorkerNode).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum LeaderNotification {
    /// Informs a [`Worker`] that a Job from the given JobGraph
    /// must be scheduled locally, and provides addresses of other
    /// [`Job`]s that this one needs to communicate with.
    ScheduleJob(JobGraphId, Job, HashMap<Job, WorkerAddress>),
    /// Informs a [`Worker`] that the JobGraph with the given ID
    /// is ready to be executed.
    ExecuteGraph(JobGraphId),
    /// Informs a [`Worker`] that it needs to shut down.
    Shutdown,
}
