use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

use bytes::BytesMut;
use serde::Deserialize;

use crate::{
    communication::{
        serializable::{Deserializable, DeserializedMessage},
        CommunicationError, SendEndpoint,
    },
    dataflow::Data,
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
impl<D> PusherT for Pusher<Arc<D>>
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
        if !self.endpoints.is_empty() {
            let start = crate::current_time_us();
            let msg = match Deserializable::decode(&mut buf)? {
                DeserializedMessage::<D>::Owned(msg) => msg,
                DeserializedMessage::<D>::Ref(msg) => msg.clone(),
            };
            println!(
                "Deserializing {} bytes took {} us",
                buf.len(),
                crate::current_time_us() - start
            );
            let msg_arc = Arc::new(msg);
            for endpoint in self.endpoints.iter_mut() {
                endpoint.send(Arc::clone(&msg_arc))?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for Box<dyn PusherT> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Box<dyn PusheT> {{ }}")
    }
}
