use std::{
    any::Any,
    collections::HashMap,
    fmt::{self, Debug},
    sync::Arc,
};

use bytes::BytesMut;
use serde::Deserialize;

use crate::{
    communication::{
        serializable::{Deserializable, DeserializedMessage, Serializable},
        CommunicationError, SendEndpoint,
    },
    dataflow::{graph::Job, Data},
};

/// Trait used to deserialize a message and send it on a collection of [`SendEndpoint`]s
/// without exposing the message's type to owner of the [`PusherT`] trait object.
pub(crate) trait PusherT: Send {
    fn as_any(&mut self) -> &mut dyn Any;
    /// Creates message from bytes and sends it to endpoints.
    fn send_from_bytes(&mut self, buf: BytesMut) -> Result<(), CommunicationError>;
}

/// Internal structure used to send data on a collection of [`SendEndpoint`]s.
#[derive(Clone)]
pub(crate) struct Pusher<D: Debug + Clone + Send> {
    // TODO: We might want to order the endpoints by the priority of their tasks.
    endpoints: HashMap<Job, SendEndpoint<D>>,
}

/// Zero-copy implementation of the pusher.
impl<D: 'static + Serializable + Send + Sync + Debug> Pusher<Arc<D>> {
    pub fn new() -> Self {
        Self {
            endpoints: HashMap::new(),
        }
    }

    pub fn add_endpoint(&mut self, job: Job, endpoint: SendEndpoint<Arc<D>>) {
        self.endpoints.insert(job, endpoint);
    }

    pub fn send(&mut self, msg: Arc<D>) -> Result<(), CommunicationError> {
        for endpoint in self.endpoints.values_mut().into_iter() {
            endpoint.send(Arc::clone(&msg))?;
        }
        Ok(())
    }
}

/// The [`PusherT`] trait is implemented only for the [`Data`] pushers.
impl<D> PusherT for Pusher<Arc<D>>
where
    for<'de> D: Data + Deserialize<'de>,
{
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn send_from_bytes(&mut self, mut buf: BytesMut) -> Result<(), CommunicationError> {
        if !self.endpoints.is_empty() {
            let msg = match Deserializable::decode(&mut buf)? {
                DeserializedMessage::<D>::Owned(msg) => msg,
                DeserializedMessage::<D>::Ref(msg) => msg.clone(),
            };
            let msg_arc = Arc::new(msg);
            self.send(msg_arc)?;
        }
        Ok(())
    }
}
