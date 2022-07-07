//! Data structures for executing and ERDOS application.
//!
//! ERDOS applications may run across one or several nodes connected via TCP,
//! as set in the [`Configuration`](crate::Configuration).
//! The [`new_app`](crate::new_app) helper function may be useful in scaling
//! from one node to many via command line arguments.
//!
//! Currently, operators are manually scheduled to a [`Node`] via the
//! [`OperatorConfig`](crate::dataflow::OperatorConfig). By default, they are
//! scheduled on node 0. We are looking into more elegant solutions for
//! scheduling operators, and hope to provide a versatile solution.

// Private submodules
mod handles;
mod leader;
mod worker_node;

// Crate-wide visible submodules
pub(crate) mod lattice;
pub(crate) mod operator_event;
pub(crate) mod resources;
pub(crate) mod worker;

// Public submodules
#[doc(hidden)]
pub mod operator_executors;

use std::{collections::HashSet, fmt, net::SocketAddr};

// Crate-wide exports.
pub(crate) use leader::Leader;
pub(crate) use resources::Resources;
use serde::{Deserialize, Serialize};
pub(crate) use worker_node::WorkerNode;

// Public exports
pub use handles::{LeaderHandle, WorkerHandle};

use crate::dataflow::graph::JobGraphId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum ExecutionState {
    Scheduled,
    Ready,
    Executing,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(usize);

impl Default for WorkerId {
    fn default() -> Self {
        Self(usize::MAX)
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<usize> for WorkerId {
    fn from(worker_id: usize) -> Self {
        Self(worker_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkerState {
    id: WorkerId,
    address: SocketAddr,
    resources: Resources,
    scheduled_job_graphs: HashSet<JobGraphId>,
}

impl WorkerState {
    fn new(id: WorkerId, address: SocketAddr, resources: Resources) -> Self {
        Self {
            id,
            address,
            resources,
            scheduled_job_graphs: HashSet::new(),
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn id(&self) -> WorkerId {
        self.id
    }

    pub(crate) fn schedule_graph(&mut self, job_graph_id: JobGraphId) {
        self.scheduled_job_graphs.insert(job_graph_id);
    }

    pub(crate) fn is_graph_scheduled(&self, job_graph_id: &JobGraphId) -> bool {
        self.scheduled_job_graphs.contains(job_graph_id)
    }
}
