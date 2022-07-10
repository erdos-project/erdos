//! Abstractions for communication between a [`Leader`] and the [`Worker`]s,
//! and between [`Worker`]s.
//!
//! This module provides support for the following two major communication patterns:
//! 1. [Control Plane] which enables a [`Leader`] to communicate with the [`Worker`]s and
//!    vice-versa. The specific commands that can be communicated by the [`Leader`] to the
//!    [`Worker`] are specified in [`LeaderNotification`], and the commands that can be
//!    communicated by the [`Worker`] to the [`Leader`] are specified in [`WorkerNotification`].
//! 2. [Data Plane] which enables multiple [`Worker`]s to communicate with each other to exchange
//!    message data. The existence of other [`Worker`]s is notified by a [`Leader`] and upon
//!    connection, they perform a [handshake procedure](EhloMetadata).
//!
//! [Control Plane]: crate::communication::control_plane
//! [Data Plane]: crate::communication::data_plane
//! [`Leader`]: crate::node::LeaderHandle
//! [`Worker`]: crate::node::WorkerHandle
//! [`LeaderNotification`]: crate::communication::control_plane::notifications::LeaderNotification
//! [`WorkerNotification`]: crate::communication::control_plane::notifications::WorkerNotification
use std::{fmt::Debug, sync::Arc};

use bytes::BytesMut;

use serde::{Deserialize, Serialize};

use crate::{dataflow::stream::StreamId, node::WorkerId};

// Private submodules
mod serializable;

// Crate-wide visible submodules
pub(crate) mod control_plane;
pub(crate) mod data_plane;
pub(crate) mod errors;

// Private imports
use serializable::Serializable;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Metadata {
    MessageMetadata(MessageMetadata),
    EhloMetadata(EhloMetadata),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MessageMetadata {
    pub stream_id: StreamId,
}

/// The metadata shared by [`Worker`](crate::node::WorkerHandle)s upon initiating a
/// [data plane](crate::communication::data_plane) connection.
///
/// This metadata is shared during the initial handshake protocol amonst `Worker`s
/// and is akin to an
/// [SMTP Extended Hello](https://tldp.org/HOWTO/Spam-Filtering-for-MX/smtpchecks.html).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EhloMetadata {
    /// The ID of the [`Worker`](crate::node::WorkerHandle) initiating the handshake.
    pub worker_id: WorkerId,
}

#[derive(Clone)]
pub(crate) enum InterWorkerMessage {
    Serialized {
        metadata: MessageMetadata,
        bytes: BytesMut,
    },
    Deserialized {
        metadata: MessageMetadata,
        data: Arc<dyn Serializable + Send + Sync>,
    },
    Ehlo {
        metadata: EhloMetadata,
    },
}

impl InterWorkerMessage {
    pub fn new_serialized(bytes: BytesMut, metadata: MessageMetadata) -> Self {
        Self::Serialized { metadata, bytes }
    }

    pub fn new_deserialized(
        data: Arc<dyn Serializable + Send + Sync>,
        stream_id: StreamId,
    ) -> Self {
        Self::Deserialized {
            metadata: MessageMetadata { stream_id },
            data,
        }
    }

    pub fn new_ehlo(metadata: EhloMetadata) -> Self {
        Self::Ehlo { metadata }
    }
}
