//! Abstractions for communicating messages across [`Worker`]s.
//! 
//! [`Worker`]: crate::node::WorkerNode

// Private Submodules
mod codec;
mod data_plane;
mod data_receiver;
mod data_sender;
mod pusher;
mod stream_manager;
mod worker_connection;

// Crate-Wide Submodules
pub(crate) mod endpoints;
pub(crate) mod notifications;

// Crate-Wide exports.
pub(crate) use data_plane::DataPlane;
pub(crate) use pusher::Pusher;
pub(crate) use stream_manager::{StreamEndpoints, StreamEndpointsT, StreamManager};
