use abomonation::{decode, encode, measure, Abomonation};
use bytes::{BufMut, BytesMut};
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

/// Trait automatically derived for all messages that derive `Serialize`.
pub trait Serializable {
    fn encode(&self) -> Result<BytesMut, CommunicationError>;
    fn encode_into(&self, buffer: &mut BytesMut) -> Result<(), CommunicationError>;
    fn serialized_size(&self) -> Result<usize, CommunicationError>;
}

impl<D> Serializable for D
where
    D: Debug + Clone + Send + Serialize,
{
    default fn encode(&self) -> Result<BytesMut, CommunicationError> {
        let serialized_msg = bincode::serialize(self).map_err(CommunicationError::from)?;
        let serialized_msg: BytesMut = BytesMut::from(&serialized_msg[..]);
        Ok(serialized_msg)
    }

    default fn encode_into(&self, buffer: &mut BytesMut) -> Result<(), CommunicationError> {
        let mut writer = buffer.writer();
        bincode::serialize_into(&mut writer, self).map_err(CommunicationError::from)
    }

    default fn serialized_size(&self) -> Result<usize, CommunicationError> {
        bincode::serialized_size(&self)
            .map(|x| x as usize)
            .map_err(CommunicationError::from)
    }
}

/// Specialized version used when messages derive `Abomonation`.
impl<D> Serializable for D
where
    D: Debug + Clone + Send + Serialize + Abomonation,
{
    fn encode(&self) -> Result<BytesMut, CommunicationError> {
        let mut serialized_msg: Vec<u8> = Vec::with_capacity(measure(self));
        unsafe {
            encode(self, &mut serialized_msg).map_err(CommunicationError::AbomonationError)?;
        }
        let serialized_msg: BytesMut = BytesMut::from(&serialized_msg[..]);
        Ok(serialized_msg)
    }

    fn encode_into(&self, buffer: &mut BytesMut) -> Result<(), CommunicationError> {
        let mut writer = buffer.writer();
        unsafe { encode(self, &mut writer).map_err(CommunicationError::AbomonationError) }
    }

    fn serialized_size(&self) -> Result<usize, CommunicationError> {
        Ok(abomonation::measure(self))
    }
}

/// Trait automatically derived for all messages that derive `Deserialize`.
pub trait Deserializable<'a>: Sized {
    fn decode(buf: &'a mut BytesMut) -> Result<DeserializedMessage<'a, Self>, CommunicationError>;
}

impl<'a, D> Deserializable<'a> for D
where
    D: Debug + Clone + Send + Deserialize<'a>,
{
    default fn decode(
        buf: &'a mut BytesMut,
    ) -> Result<DeserializedMessage<'a, D>, CommunicationError> {
        let msg: D = bincode::deserialize(buf).map_err(|e| CommunicationError::from(e))?;
        Ok(DeserializedMessage::Owned(msg))
    }
}

/// Specialized version used when messages derive `Abomonation`.
impl<'a, D> Deserializable<'a> for D
where
    D: Debug + Clone + Send + Deserialize<'a> + Abomonation,
{
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
