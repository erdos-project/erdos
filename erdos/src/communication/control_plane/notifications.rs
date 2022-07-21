//! A collection of enums that specify the communication patterns between
//! [`Leader`](crate::node::Leader), [`Worker`](crate::node::WorkerNode)
//! and [`Driver`](crate::dataflow::graph::Job::Driver)s.
use std::{collections::HashMap, error::Error, fmt, net::SocketAddr};

use serde::{Deserialize, Serialize};

use crate::{
    dataflow::graph::{AbstractJobGraph, Job, JobGraph, JobGraphId},
    node::{ExecutionState, Resources, WorkerId},
    Uuid,
};

/// The identifier to uniquely identify each [`Query`](WorkerNotification::Query).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct QueryId(pub Uuid);

/// The error returned by the Leader if a [`Query`](WorkerNotification::Query) cannot be
/// answered correctly by the Leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueryError(pub String);
impl Error for QueryError {}
impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QueryError({})", self.0)
    }
}

/// An enum specifying the types of queries that a [`Worker`] can ask a [`Leader`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum QueryType {
    /// Request the `Leader` for the status of the `JobGraph` with the given ID.
    JobGraphStatus(JobGraphId),
}

/// An enum specifying the responses to the queries that a [`Worker`] can ask a [`Leader`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum QueryResponseType {
    /// Responds to the [`QueryType::JobGraphStatus`] query with the `ExecutionState`.
    JobGraphStatus(JobGraphId, Result<ExecutionState, QueryError>),
}

/// An enum specifying the notifications that a [`Driver`](crate::dataflow::graph::Job::Driver)
/// can send to a [`Worker`](crate::node::WorkerNode).
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum DriverNotification {
    /// Notifies the `Worker` to compile and register the graph with itself.
    RegisterGraph(JobGraph),
    /// Notifies the `Worker` to register the graph with itself, and submit to the `Leader`.
    SubmitGraph(JobGraphId),
    /// Requests the `Worker` to respond to a [`Query`](QueryType).
    Query(QueryType),
    /// Response to a `Query` sent by the `Driver`.
    QueryResponse(QueryResponseType),
    /// Notifies the `Worker` to shutdown.
    Shutdown,
}

/// An enum specifying the notifications that a [`Worker`](crate::node::WorkerNode)
/// can send to a [`Leader`](crate::node::Leader).
#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum WorkerNotification {
    /// Informs the `Leader` that a new `Worker` has been initialized and provides
    /// information about the ID, Address and Resources of the Worker.
    Initialized(WorkerId, SocketAddr, Resources),
    /// Informs the `Leader` that the [`Job`](crate::dataflow::graph::Job) from the
    /// [`Graph`](crate::dataflow::graph::Graph) with the provided ID is ready for
    /// execution.
    JobUpdate(JobGraphId, Job, ExecutionState),
    /// Requests the `Leader` to respond to the [`Query`](QueryType).
    Query(QueryId, QueryType),
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
    /// The [`Job`] originates from a remote address with the given ID and Socket address.
    Remote(WorkerId, SocketAddr),
    /// The [`Job`] originates on the local Worker.
    Local,
}

/// A [`LeaderNotification`] specifies the notifications that a
/// [`LeaderNode`](crate::node::LeaderNode) can send to a [`WorkerNode`](crate::node::WorkerNode).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum LeaderNotification {
    /// Informs a [`Worker`] that a Job from the given JobGraph must be scheduled locally,
    /// and provides addresses of other [`Job`]s that this one needs to communicate with.
    ScheduleJob(JobGraphId, Job, HashMap<Job, WorkerAddress>),
    /// Informs a [`Worker`] that the JobGraph with the given ID is ready to be executed.
    ExecuteGraph(JobGraphId),
    /// Responds to a [`Query`](WorkerNotification::Query) sent by the `Worker`.
    QueryResponse(QueryId, QueryResponseType),
    /// Informs a [`Worker`] that it needs to shut down.
    Shutdown,
}
