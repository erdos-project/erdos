use std::{fmt::Debug, sync::Arc};

use bytes::BytesMut;

use serde::{Deserialize, Serialize};

use crate::{
    dataflow::stream::StreamId,
    node::{NodeId, WorkerId},
    OperatorId,
};

// Private submodules
mod control_message_handler;
mod endpoints;
mod errors;
mod serializable;

// Crate-wide visible submodules
pub(crate) mod control_plane;
pub(crate) mod data_plane;
pub(crate) mod pusher;

// Private imports
use serializable::Serializable;

// Module-wide exports
pub(crate) use control_message_handler::ControlMessageHandler;
pub(crate) use data_plane::codec::MessageCodec;
pub(crate) use errors::{CodecError, CommunicationError, TryRecvError};
pub(crate) use pusher::{Pusher, PusherT};

// Crate-wide exports
pub(crate) use endpoints::{RecvEndpoint, SendEndpoint};

/// Message sent between nodes in order to coordinate node and operator initialization.
#[derive(Debug, Clone)]
pub enum ControlMessage {
    AllOperatorsInitializedOnNode(NodeId),
    OperatorInitialized(OperatorId),
    RunOperator(OperatorId),
    DataSenderInitialized(NodeId),
    DataReceiverInitialized(NodeId),
    PusherUpdate(StreamId),
    PusherUpdated(StreamId),
}

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
