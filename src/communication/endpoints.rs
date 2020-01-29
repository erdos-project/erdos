use bytes::BytesMut;
use std::fmt::Debug;
use tokio::sync::mpsc;

use crate::{
    communication::{CommunicationError, SerializedMessage, TryRecvError},
    dataflow::stream::StreamId,
};

/// Endpoint to be used to send messages between operators.
#[derive(Clone)]
pub enum SendEndpoint<D: Clone + Send + Debug> {
    /// Send messages between operators running in the same process.
    InterThread(std::sync::mpsc::Sender<D>),
    /// Send messages between operator executors and network threads.
    InterProcess(StreamId, mpsc::UnboundedSender<SerializedMessage>),
}

impl<D: Clone + Send + Debug> SendEndpoint<D> {
    /// To be used with `SendEndpoint::InterThread` endpoints. We do not implement this method for
    /// other types of endpoints because we do not want to encourage usages of endpoints in which
    /// the messages is serialized for each endpoint.
    pub fn send(&mut self, msg: D) -> Result<(), CommunicationError> {
        match self {
            Self::InterThread(sender) => sender.send(msg).map_err(|e| CommunicationError::from(e)),
            Self::InterProcess(_, _) => Err(CommunicationError::SerializeNotImplemented),
        }
    }

    /// To be used with `SendEndpoint::InterProcess` endpoints. We do not implement this method for
    /// other types of endpoints because we do not want to encourage usages of endpoints in which
    /// the messages is deserialized for each endpoint.
    pub fn send_from_bytes(&mut self, data: BytesMut) -> Result<(), CommunicationError> {
        match self {
            Self::InterThread(_) => Err(CommunicationError::DeserializeNotImplemented),
            Self::InterProcess(stream_id, sender) => {
                let msg = SerializedMessage::new(data, *stream_id);
                sender.send(msg).map_err(CommunicationError::from)
            }
        }
    }
}

/// Endpoint to be used to receive messages.
pub enum RecvEndpoint<D: Clone + Send + Debug> {
    InterThread(std::sync::mpsc::Receiver<D>),
}

impl<D: Clone + Send + Debug> RecvEndpoint<D> {
    /// Blocking read of a new message.
    pub fn read(&mut self) -> Result<D, CommunicationError> {
        match self {
            Self::InterThread(receiver) => receiver
                .recv()
                .map_err(|_| CommunicationError::Disconnected),
        }
    }

    /// Non-blocking read of a new message. Returns `TryRecvError::Empty` if no message is available.
    pub fn try_read(&mut self) -> Result<D, TryRecvError> {
        match self {
            Self::InterThread(receiver) => receiver.try_recv().map_err(TryRecvError::from),
        }
    }
}
