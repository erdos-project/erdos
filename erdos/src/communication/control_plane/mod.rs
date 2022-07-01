//! Abstractions for communication between ERDOS [`Worker`]s and the [`Leader`].
//! 
//! [`Worker`]: crate::node::WorkerNode
//! [`Leader`]: crate::node::Leader

// Private Modules.
mod codec;

// Crate-Wide Public Modules.
pub(crate) mod notifications;

// Crate-Wide Exports.
pub(crate) use codec::ControlPlaneCodec;