use futures::future;
use futures::stream::SplitSink;
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::UnboundedReceiver, Mutex};
use tokio::{codec::Framed, net::TcpStream, prelude::*, self};

use crate::communication::{
    CommunicationError, ControlMessage, ControlMessageCodec, MessageCodec, SerializedMessage,
};
use crate::node::node::NodeId;
use crate::scheduler::endpoints_manager::ChannelsToSenders;

#[allow(dead_code)]
/// Listens on a `tokio::sync::mpsc` channel, and sends received messages on the network.
pub struct ERDOSSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Framed TCP write sink.
    sink: SplitSink<Framed<TcpStream, MessageCodec>, SerializedMessage>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: UnboundedReceiver<SerializedMessage>,
}

impl ERDOSSender {
    pub async fn new(
        node_id: NodeId,
        sink: SplitSink<Framed<TcpStream, MessageCodec>, SerializedMessage>,
        channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
    ) -> Self {
        // Create a channel for this stream.
        let (tx, rx) = mpsc::unbounded_channel();
        // Add entry in the shared state map.
        channels_to_senders.lock().await.add_sender(node_id, tx);
        Self { node_id, sink, rx }
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        loop {
            match self.rx.recv().await {
                Some(msg) => {
                    if let Err(e) = self.sink.send(msg).await.map_err(CommunicationError::from) {
                        return Err(e);
                    }
                }
                None => return Err(CommunicationError::Disconnected),
            }
        }
    }
}

/// Sends messages received from operator executors to other nodes.
/// The function launches a task for each TCP sink. Each task listens
/// on a mpsc channel for new `SerializedMessages` messages, which it
/// forwards on the TCP stream.
pub async fn run_senders(mut senders: Vec<ERDOSSender>) -> Result<(), CommunicationError> {
    // Waits until all futures complete. This code will only be reached
    // when all the mpsc channels are closed.
    future::join_all(senders.iter_mut().map(|sender| sender.run())).await;
    Ok(())
}

#[allow(dead_code)]
/// Listens for control messages on a `tokio::sync::mpsc` channel, and sends received messages on the network.
pub struct ControlSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Framed TCP write sink.
    sink: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: UnboundedReceiver<ControlMessage>,
}

impl ControlSender {
    pub fn new(
        node_id: NodeId,
        sink: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
        rx: UnboundedReceiver<ControlMessage>,
    ) -> Self {
        Self { node_id, sink, rx }
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        loop {
            match self.rx.recv().await {
                Some(msg) => {
                    if let Err(e) = self.sink.send(msg).await.map_err(CommunicationError::from) {
                        return Err(e);
                    }
                }
                None => { return Err(CommunicationError::Disconnected); },
            }
        }
    }
}

/// Sends messages received from the control handler other nodes.
/// The function launches a task for each TCP sink. Each task listens
/// on a mpsc channel for new `ControlMessage`s, which it
/// forwards on the TCP stream.
pub async fn run_control_senders(mut senders: Vec<ControlSender>) -> Result<(), CommunicationError> {
    // Waits until all futures complete. This code will only be reached
    // when all the mpsc channels are closed.
    future::join_all(senders.iter_mut().map(|sender| sender.run())).await;
    Ok(())
}
