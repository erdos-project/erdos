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
#[allow(clippy::module_inception)]
mod node;
mod worker_node;

// Crate-wide visible submodules
pub(crate) mod lattice;
pub(crate) mod operator_event;
pub(crate) mod resources;
pub(crate) mod worker;

// Public submodules
#[doc(hidden)]
pub mod operator_executors;

// Crate-wide exports.
pub(crate) use leader::LeaderNode;
pub(crate) use resources::Resources;
pub(crate) use worker_node::{WorkerNode, WorkerState};

// Public exports
pub use handles::{LeaderHandle, WorkerHandle};
pub use node::{Node, NodeHandle, NodeId};
