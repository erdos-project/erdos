// Modules visible outside of the node module.
pub mod node;
pub mod operator_event;
pub mod operator_executor;
pub mod lattice;

// Re-export structs as if they were defined here.
pub use node::{Node, NodeHandle, NodeId};
