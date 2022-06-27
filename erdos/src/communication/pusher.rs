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
    dataflow::{graph::Job, stream::StreamId, Data},
};

/// Trait used to deserialize a message and send it on a collection of [`SendEndpoint`]s
/// without exposing the message's type to owner of the [`PusherT`] trait object.
pub(crate) trait PusherT: Send {
    /// Retrieve the ID of the Stream for which this Pusher was created.
    fn stream_id(&self) -> StreamId;

    /// Retrieve the Jobs for which an endpoint is registered on this Pusher.
    fn get_jobs(&self) -> Vec<Job>;

    fn as_any(&mut self) -> &mut dyn Any;

    /// Creates message from bytes and sends it to endpoints.
    fn send_from_bytes(&mut self, buf: BytesMut) -> Result<(), CommunicationError>;
}

/// Internal structure used to send data on a collection of [`SendEndpoint`]s.
#[derive(Clone)]
pub(crate) struct Pusher<D: Debug + Clone + Send> {
    stream_id: StreamId,
    // TODO: We might want to order the endpoints by the priority of their tasks.
    endpoints: HashMap<Job, SendEndpoint<D>>,
}

/// Zero-copy implementation of the pusher.
impl<D: 'static + Serializable + Send + Sync + Debug> Pusher<Arc<D>> {
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
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
    fn get_jobs(&self) -> Vec<Job> {
        self.endpoints.keys().into_iter().cloned().collect()
    }

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

    fn stream_id(&self) -> StreamId {
        self.stream_id
    }
}

impl fmt::Debug for dyn PusherT {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PusherT (ID={}, Jobs={:?})",
            self.stream_id(),
            self.get_jobs()
        )
    }
}
