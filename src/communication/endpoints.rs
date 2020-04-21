use std::fmt::Debug;
use tokio::sync::mpsc;

use crate::{
    communication::{CommunicationError, InterProcessMessage, TryRecvError},
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

impl<D: Clone + Send + Debug> SendEndpoint<D> {
    /// To be used with `SendEndpoint::InterThread` endpoints. We do not implement this method for
    /// other types of endpoints because we do not want to encourage usages of endpoints in which
    /// the messages is serialized for each endpoint.
    pub fn send(&mut self, msg: D) -> Result<(), CommunicationError> {
        match self {
            Self::InterThread(sender) => sender.send(msg).map_err(CommunicationError::from),
            Self::InterProcess(_, _) => {
                panic!("Used `send` with `SendEndpoint::InterProcess endpoint.")
            }
        }
    }

    pub fn send_inter_process(
        &mut self,
        msg: InterProcessMessage,
    ) -> Result<(), CommunicationError> {
        match self {
            Self::InterThread(_) => {
                panic!("Used `send_inter_process` with `SendEndpoint::InterThread` endpoint.")
            }
            Self::InterProcess(_, sender) => sender.send(msg).map_err(CommunicationError::from),
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
