use bytes::BytesMut;
use serde::Deserialize;
use std::{any::Any, collections::HashMap, fmt::Debug};
use tokio::{prelude::*, sync::mpsc::UnboundedSender};

use crate::{
    communication::{
        serializable::{DeserializedMessage, Serializable},
        CommunicationError, ControlMessage, SendEndpoint,
    },
    dataflow::Data,
    node::NodeId,
};

/// Trait used to wrap a bunch of SendEndpoints of different types.
pub trait PusherT: Send {
    fn as_any(&mut self) -> &mut dyn Any;
    /// To be used to clone a boxed pusher.
    fn box_clone(&self) -> Box<dyn PusherT>;
    /// Creates message from bytes and sends it to endpoints.
    fn send(&mut self, buf: BytesMut) -> Result<(), CommunicationError>;
}

/// Internal structure used to send data to other operators or threads.
#[derive(Clone)]
pub struct Pusher<D: Debug + Clone + Send> {
    endpoints: Vec<SendEndpoint<D>>,
}

impl<D: Debug + Clone + Send> Pusher<D> {
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
        }
    }

    /// Adds a SendEndpoint to the pusher.
    pub fn add_endpoint(&mut self, endpoint: SendEndpoint<D>) {
        self.endpoints.push(endpoint);
    }
}

impl Clone for Box<dyn PusherT> {
    /// Clones a boxed pusher.
    fn clone(&self) -> Box<dyn PusherT> {
        self.box_clone()
    }
}

/// The `PusherT` trait is implemented only for the `Data` pushers.
impl<D> PusherT for Pusher<D>
where
    for<'de> D: Data + Deserialize<'de>,
{
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn PusherT> {
        Box::new((*self).clone())
    }

    fn send(&mut self, mut buf: BytesMut) -> Result<(), CommunicationError> {
        if self.endpoints.len() > 0 {
            match Serializable::decode(&mut buf)? {
                DeserializedMessage::<D>::Owned(msg) => {
                    for i in 1..self.endpoints.len() {
                        self.endpoints[i].send(msg.clone())?;
                    }
                    self.endpoints[0].send(msg)?;
                }
                DeserializedMessage::<D>::Ref(msg) => {
                    for i in 0..self.endpoints.len() {
                        self.endpoints[i].send(msg.clone())?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Internal structure to send control messages to other processes
pub struct ControlPusher {
    channels_to_senders: HashMap<NodeId, UnboundedSender<ControlMessage>>,
}

impl ControlPusher {
    pub fn new(channels_to_senders: HashMap<NodeId, UnboundedSender<ControlMessage>>) -> Self {
        Self {
            channels_to_senders,
        }
    }

    pub fn send(&mut self, node_id: NodeId, msg: ControlMessage) -> Result<(), CommunicationError> {
        match self.channels_to_senders.get_mut(&node_id) {
            Some(channel) => channel.try_send(msg).map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub fn broadcast(&mut self, msg: ControlMessage) -> Result<(), CommunicationError> {
        for channel in self.channels_to_senders.values_mut() {
            channel.try_send(msg.clone()).map_err(CommunicationError::from)?;
        }
        Ok(())
    }
}
