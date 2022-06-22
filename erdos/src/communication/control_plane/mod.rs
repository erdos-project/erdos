//! Data structures for control plane communication between ERDOS Workers and the Leader.

// Private Modules.
mod codec;

// Crate-Wide Public Modules.
pub(crate) mod notifications;

// Crate-Wide Exports.
pub(crate) use codec::ControlPlaneCodec;