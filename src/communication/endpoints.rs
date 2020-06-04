use std::{fmt::Debug, sync::Arc};
use tokio::sync::mpsc;

use crate::{
    communication::{CommunicationError, InterProcessMessage, Serializable, TryRecvError},
    dataflow::stream::StreamId,
};

/// Endpoint to be used to send messages between operators.
#[derive(Clone)]
pub enum SendEndpoint<D: Clone + Send + Debug> {
    /// Send messages between operators running in the same process.
    InterThread(mpsc::UnboundedSender<D>),
    /// Send messages between operator executors and network threads.
    InterProcess(StreamId, mpsc::UnboundedSender<InterProcessMessage>),
}

/// Zero-copy implementation of the endpoint.
impl<D: 'static + Serializable + Send + Sync + Debug> SendEndpoint<Arc<D>> {
    pub fn send(&mut self, msg: Arc<D>) -> Result<(), CommunicationError> {
        match self {
            Self::InterThread(sender) => sender.send(msg).map_err(CommunicationError::from),
            Self::InterProcess(stream_id, sender) => sender
                .send(InterProcessMessage::new_deserialized(msg, *stream_id))
                .map_err(CommunicationError::from),
        }
    }
}

/// Endpoint to be used to receive messages.
pub enum RecvEndpoint<D: Clone + Send + Debug> {
    InterThread(mpsc::UnboundedReceiver<D>),
}

impl<D: Clone + Send + Debug> RecvEndpoint<D> {
    /// Aync read of a new message.
    pub async fn read(&mut self) -> Result<D, CommunicationError> {
        match self {
            Self::InterThread(receiver) => receiver
                .recv()
                .await
                .ok_or(CommunicationError::Disconnected),
        }
    }

    /// Non-blocking read of a new message. Returns `TryRecvError::Empty` if no message is available.
    pub fn try_read(&mut self) -> Result<D, TryRecvError> {
        match self {
            Self::InterThread(receiver) => receiver.try_recv().map_err(TryRecvError::from),
        }
    }
}
