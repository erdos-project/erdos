use futures::{future, stream::SplitStream};
use futures_util::stream::StreamExt;
use std::{
    collections::HashMap,
    sync::{mpsc, Arc},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc::UnboundedSender, Mutex},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        CommunicationError, ControlMessage, ControlMessageCodec, MessageCodec, PusherT,
    },
    dataflow::stream::StreamId,
    node::node::NodeId,
    scheduler::endpoints_manager::ChannelsToReceivers,
};

/// Listens on a TCP stream, and pushes messages it receives to operator executors.
#[allow(dead_code)]
pub struct DataReceiver {
    /// The id of the node the stream is receiving data from.
    node_id: NodeId,
    /// Framed TCP read stream.
    stream: SplitStream<Framed<TcpStream, MessageCodec>>,
    /// Channel receiver on which new pusher updates are received.
    rx: mpsc::Receiver<(StreamId, Box<dyn PusherT>)>,
    /// Mapping between stream id to pushers.
    stream_id_to_pusher: HashMap<StreamId, Box<dyn PusherT>>,
}

impl DataReceiver {
    pub async fn new(
        node_id: NodeId,
        stream: SplitStream<Framed<TcpStream, MessageCodec>>,
        channels_to_receivers: Arc<Mutex<ChannelsToReceivers>>,
    ) -> Self {
        // Create a channel for this stream.
        let (tx, rx) = mpsc::channel();
        // Add entry in the shared state vector.
        channels_to_receivers.lock().await.add_sender(tx);
        Self {
            node_id,
            stream,
            rx,
            stream_id_to_pusher: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        while let Some(res) = self.stream.next().await {
            match res {
                // Push the message to the listening operator executors.
                Ok(msg) => {
                    // Update pushers before we send the message.
                    // Note: we may want to update the pushers less frequently.
                    self.update_pushers();
                    // Send the message.
                    match self.stream_id_to_pusher.get_mut(&msg.header.stream_id) {
                        Some(pusher) => {
                            if let Err(e) = pusher.send(msg.data) {
                                return Err(e);
                            }
                        }
                        None => panic!(
                            "Receiver does not have any pushers. \
                             Race condition during data-flow reconfiguration."
                        ),
                    }
                }
                Err(e) => return Err(CommunicationError::from(e)),
            }
        }
        Ok(())
    }

    fn update_pushers(&mut self) {
        // Execute while we still have pusher updates.
        while let Ok((stream_id, pusher)) = self.rx.try_recv() {
            self.stream_id_to_pusher.insert(stream_id, pusher);
        }
    }
}

/// Receives TCP messages, and pushes them to operators endpoints.
/// The function receives a vector of framed TCP receiver halves.
/// It launches a task that listens for new messages for each TCP connection.
pub async fn run_receivers(mut receivers: Vec<DataReceiver>) -> Result<(), CommunicationError> {
    // Wait for all futures to finish. It will happen only when all streams are closed.
    future::join_all(receivers.iter_mut().map(|receiver| receiver.run())).await;
    Ok(())
}

/// Listens on a TCP stream, and pushes control messages it receives to the node.
#[allow(dead_code)]
pub struct ControlReceiver {
    /// The id of the node the stream is receiving data from.
    node_id: NodeId,
    /// Framed TCP read stream.
    stream: SplitStream<Framed<TcpStream, ControlMessageCodec>>,
    /// Mapping between stream id to pushers.
    channel_to_handler: UnboundedSender<ControlMessage>,
}

impl ControlReceiver {
    pub fn new(
        node_id: NodeId,
        stream: SplitStream<Framed<TcpStream, ControlMessageCodec>>,
        channel_to_handler: UnboundedSender<ControlMessage>,
    ) -> Self {
        Self {
            node_id,
            stream,
            channel_to_handler,
        }
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        // TODO: update `self.channel_to_handler` for up-to-date mappings
        // between channels and handlers (e.g. for fault-tolerance).
        while let Some(res) = self.stream.next().await {
            match res {
                Ok(msg) => {
                    self.channel_to_handler
                        .send(msg)
                        .map_err(CommunicationError::from)?;
                }
                Err(e) => return Err(CommunicationError::from(e)),
            }
        }
        Ok(())
    }
}

/// Receives TCP messages, and pushes them to the ControlHandler
/// The function receives a vector of framed TCP receiver halves.
/// It launches a task that listens for new messages for each TCP connection.
pub async fn run_control_receivers(
    mut receivers: Vec<ControlReceiver>,
) -> Result<(), CommunicationError> {
    // Wait for all futures to finish. It will happen only when all streams are closed.
    future::join_all(receivers.iter_mut().map(|receiver| receiver.run())).await;
    Ok(())
}
