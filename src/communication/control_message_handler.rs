use std::collections::HashMap;

use slog::Logger;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::node::NodeId;

use super::{CommunicationError, ControlMessage};

// TODO: update `channels_to_nodes` for fault tolerance in case nodes to go down.
pub struct ControlMessageHandler {
    /// Logger for error messages.
    logger: Logger,
    /// Sender to clone so other tasks can send messages to `self.rx`.
    tx: UnboundedSender<ControlMessage>,
    /// Receiver for all `ControlMessage`s
    rx: UnboundedReceiver<ControlMessage>,
    channels_to_control_senders: HashMap<NodeId, UnboundedSender<ControlMessage>>,
    channels_to_control_receivers: HashMap<NodeId, UnboundedSender<ControlMessage>>,
    channels_to_data_senders: HashMap<NodeId, UnboundedSender<ControlMessage>>,
    channels_to_data_receivers: HashMap<NodeId, UnboundedSender<ControlMessage>>,
    channels_to_nodes: HashMap<NodeId, UnboundedSender<ControlMessage>>,
}

impl ControlMessageHandler {
    pub fn new(logger: Logger) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            logger,
            tx,
            rx,
            channels_to_control_senders: HashMap::new(),
            channels_to_control_receivers: HashMap::new(),
            channels_to_data_senders: HashMap::new(),
            channels_to_data_receivers: HashMap::new(),
            channels_to_nodes: HashMap::new(),
        }
    }

    pub fn add_channel_to_control_sender(
        &mut self,
        node_id: NodeId,
        tx: UnboundedSender<ControlMessage>,
    ) {
        if let Some(_) = self.channels_to_control_senders.insert(node_id, tx) {
            error!(
                self.logger,
                "ControlMessageHandler: overwrote channel to control sender for node {}", node_id
            );
        }
    }

    pub fn send_to_control_sender(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_control_senders.get_mut(&node_id) {
            Some(tx) => tx.send(msg).map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub fn broadcast_to_control_senders(
        &mut self,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        for tx in self.channels_to_control_senders.values_mut() {
            tx.send(msg.clone()).map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn add_channel_to_control_receiver(
        &mut self,
        node_id: NodeId,
        tx: UnboundedSender<ControlMessage>,
    ) {
        if let Some(_) = self.channels_to_control_receivers.insert(node_id, tx) {
            error!(
                self.logger,
                "ControlMessageHandler: overwrote channel to control receiver for node {}", node_id
            );
        }
    }

    pub fn send_to_control_receiver(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_control_receivers.get_mut(&node_id) {
            Some(tx) => tx.send(msg).map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub fn broadcast_to_control_receivers(
        &mut self,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        for tx in self.channels_to_control_receivers.values_mut() {
            tx.send(msg.clone()).map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn add_channel_to_data_sender(
        &mut self,
        node_id: NodeId,
        tx: UnboundedSender<ControlMessage>,
    ) {
        if let Some(_) = self.channels_to_data_senders.insert(node_id, tx) {
            error!(
                self.logger,
                "ControlMessageHandler: overwrote channel to data sender for node {}", node_id
            );
        }
    }

    pub fn send_to_data_sender(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_data_senders.get_mut(&node_id) {
            Some(tx) => tx.send(msg).map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub fn broadcast_to_data_senders(
        &mut self,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        for tx in self.channels_to_data_senders.values_mut() {
            tx.send(msg.clone()).map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn add_channel_to_data_receiver(
        &mut self,
        node_id: NodeId,
        tx: UnboundedSender<ControlMessage>,
    ) {
        if let Some(_) = self.channels_to_data_receivers.insert(node_id, tx) {
            error!(
                self.logger,
                "ControlMessageHandler: overwrote channel to data receiver for node {}", node_id
            );
        }
    }

    pub fn send_to_data_receiver(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_data_receivers.get_mut(&node_id) {
            Some(tx) => tx.send(msg).map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub fn broadcast_to_data_receivers(
        &mut self,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        for tx in self.channels_to_data_receivers.values_mut() {
            tx.send(msg.clone()).map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn add_channel_to_node(&mut self, node_id: NodeId, tx: UnboundedSender<ControlMessage>) {
        self.channels_to_nodes.insert(node_id, tx);
    }

    pub fn send_to_node(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_nodes.get_mut(&node_id) {
            Some(tx) => tx.send(msg).map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub fn broadcast_to_nodes(&mut self, msg: ControlMessage) -> Result<(), CommunicationError> {
        for tx in self.channels_to_nodes.values_mut() {
            tx.send(msg.clone()).map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn get_channel_to_handler(&self) -> UnboundedSender<ControlMessage> {
        self.tx.clone()
    }

    pub async fn read(&mut self) -> Result<ControlMessage, CommunicationError> {
        self.rx.recv().await.ok_or(CommunicationError::Disconnected)
    }
}
