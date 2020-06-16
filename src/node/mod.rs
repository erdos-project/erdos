// Private submodules
mod lattice;
mod node;

// Crate-wide visible submodules
pub(crate) mod operator_event;

// Public submodules
pub mod operator_executor;

// Public exports
pub use node::{Node, NodeHandle, NodeId};
