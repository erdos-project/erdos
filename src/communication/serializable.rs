use abomonation::{decode, encode, measure, Abomonation};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    io::{Error, ErrorKind},
};

use crate::communication::CommunicationError;

/// Wrapper around a deserialized message. The wrapper can either own the deserialized
/// message or store a reference to it.
pub enum DeserializedMessage<'a, T> {
    Ref(&'a T),
    Owned(T),
}

/// Trait automatically derived for all messages that derive `Serialize` and `Deserialize`.
pub trait Serializable<'a>: Sized {
    fn encode(&self) -> Result<BytesMut, CommunicationError>;
    fn decode(buf: &'a mut BytesMut) -> Result<DeserializedMessage<'a, Self>, CommunicationError>;
}

impl<'a, D> Serializable<'a> for D
where
    D: Debug + Clone + Send + Serialize + Deserialize<'a>,
{
    default fn encode(&self) -> Result<BytesMut, CommunicationError> {
        let serialized_msg = bincode::serialize(self).map_err(|e| CommunicationError::from(e))?;
        // TODO: check whether this introduces extra copies
        // On v0.5.4, BytesMut does not implement From<Vec<u8>>
        let serialized_msg: BytesMut = BytesMut::from(&serialized_msg[..]);
        Ok(serialized_msg)
    }

    default fn decode(
        buf: &'a mut BytesMut,
    ) -> Result<DeserializedMessage<'a, D>, CommunicationError> {
        let msg: D = bincode::deserialize(buf).map_err(|e| CommunicationError::from(e))?;
        Ok(DeserializedMessage::Owned(msg))
    }
}

/// Specialized version used when messages derive `Abomonation`.
impl<'a, D> Serializable<'a> for D
where
    D: Debug + Clone + Send + Serialize + Deserialize<'a> + Abomonation,
{
    fn encode(&self) -> Result<BytesMut, CommunicationError> {
        let mut serialized_msg: Vec<u8> = Vec::with_capacity(measure(self));
        unsafe {
            encode(self, &mut serialized_msg)
                .map_err(|e| CommunicationError::AbomonationError(e))?;
        }
        // TODO: check whether this introduces extra copies
        // On v0.5.4, BytesMut does not implement From<Vec<u8>>
        let serialized_msg: BytesMut = BytesMut::from(&serialized_msg[..]);
        Ok(serialized_msg)
    }

    fn decode(buf: &'a mut BytesMut) -> Result<DeserializedMessage<'a, D>, CommunicationError> {
        let (msg, _) = {
            unsafe {
                match decode::<D>(buf.as_mut()) {
                    Some(msg) => msg,
                    None => {
                        return Err(CommunicationError::AbomonationError(Error::new(
                            ErrorKind::Other,
                            "Deserialization failed",
                        )))
                    }
                }
            }
        };
        Ok(DeserializedMessage::Ref(msg))
    }
}
