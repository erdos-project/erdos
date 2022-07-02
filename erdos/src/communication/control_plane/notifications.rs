//! A collection of enums that specify the communication patterns between
//! [`Leader`](crate::node::Leader), [`Worker`](crate::node::WorkerNode)
//! and [`Driver`](crate::dataflow::graph::Job::Driver)s.
use std::{collections::HashMap, net::SocketAddr};

use serde::{Deserialize, Serialize};

use crate::{
    dataflow::graph::{AbstractJobGraph, Job, JobGraph, JobGraphId},
    node::{JobState, Resources, WorkerId},
};

/// An enum specifying the notifications that a [`Driver`](crate::dataflow::graph::Job::Driver)
/// can send to a [`Worker`](crate::node::WorkerNode).
#[derive(Debug, Clone)]
pub(crate) enum DriverNotification {
    /// Notifies the `Worker` to compile and register the graph with itself.
    RegisterGraph(JobGraph),
    /// Notifies the `Worker` to register the graph with itself, and submit to the `Leader`.
    SubmitGraph(JobGraphId),
    /// Notifies the `Worker` to shutdown.
    Shutdown,
}

/// An enum specifying the notifications that a [`Worker`](crate::node::WorkerNode)
/// can send to a [`Leader`](crate::node::Leader).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum WorkerNotification {
    /// Informs the `Leader` that a new `Worker` has been initialized and provides
    /// information about the ID, Address and Resources of the Worker.
    Initialized(WorkerId, SocketAddr, Resources),
    /// Informs the `Leader` that the [`Job`](crate::dataflow::graph::Job) from the
    /// [`Graph`](crate::dataflow::graph::Graph) with the provided ID is ready for
    /// execution.
    JobUpdate(JobGraphId, Job, JobState),
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
