use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    communication::{InterProcessMessage, PusherT},
    dataflow::stream::StreamId,
    node::NodeId,
};

/// Wrapper used to update pushers in the TCP receiving.
///
/// Stores `mpsc::Sender`s to receivers on which `PusherT` can be sent to inform
/// the receivers that data should be sent to ne operators.
pub struct ChannelsToReceivers {
    // We do not use a tokio::mpsc::UnboundedSender because that only provides a blocking API.
    // It does not allow us to just check if the channel has a new message. We need this API in
    // the receivers, which regularly check if there are new pushers available.
    senders: Vec<UnboundedSender<(StreamId, Box<dyn PusherT>)>>,
}

impl ChannelsToReceivers {
    pub fn new() -> Self {
        ChannelsToReceivers {
            senders: Vec::new(),
        }
    }

    /// Adds a `mpsc::Sender` to a new receiver thread.
    pub fn add_sender(&mut self, sender: UnboundedSender<(StreamId, Box<dyn PusherT>)>) {
        self.senders.push(sender);
    }

    /// Updates the receivers about the existance of a new operator.
    ///
    /// It sends a `PusherT` to message on all receiving threads.
    pub fn send(&mut self, stream_id: StreamId, pusher: Box<dyn PusherT>) {
        for sender in self.senders.iter_mut() {
            let msg = (stream_id.clone(), pusher.clone());
            sender.send(msg).unwrap();
        }
    }
}

/// Wrapper used to store mappings between node ids and `mpsc::UnboundedSender` to sender threads.
pub struct ChannelsToSenders {
    /// The ith sender corresponds to a TCP connection to the ith node.
    senders: HashMap<NodeId, UnboundedSender<InterProcessMessage>>,
}

impl ChannelsToSenders {
    pub fn new() -> Self {
        ChannelsToSenders {
            senders: HashMap::new(),
        }
    }

    /// Adds a `mpsc::UnboundedSender` to a node.
    pub fn add_sender(
        &mut self,
        node_id: NodeId,
        sender: tokio::sync::mpsc::UnboundedSender<InterProcessMessage>,
    ) {
        self.senders.insert(node_id, sender);
    }

    /// Returns the associated `mpsc::UnboundedSender` for a given node.
    pub fn clone_channel(
        &self,
        node_id: NodeId,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<InterProcessMessage>> {
        self.senders.get(&node_id).map(|c| c.clone())
    }
}
