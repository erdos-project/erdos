use std::{fmt::Debug, sync::Arc};

use bytes::BytesMut;

use serde::{Deserialize, Serialize};

use crate::{dataflow::stream::StreamId, node::WorkerId};

// Private submodules
mod errors;
mod serializable;

// Crate-wide visible submodules
pub(crate) mod control_plane;
pub(crate) mod data_plane;

// Private imports
use serializable::Serializable;

// Module-wide exports
pub(crate) use data_plane::{
    endpoints::{RecvEndpoint, SendEndpoint},
    MessageCodec, Pusher,
};
pub(crate) use errors::{CodecError, CommunicationError, TryRecvError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Metadata {
    MessageMetadata(MessageMetadata),
    EhloMetadata(EhloMetadata),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMetadata {
    pub stream_id: StreamId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EhloMetadata {
    pub worker_id: WorkerId,
}

#[derive(Clone)]
pub enum InterProcessMessage {
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

impl InterProcessMessage {
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
