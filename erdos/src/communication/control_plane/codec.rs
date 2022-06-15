use std::marker::PhantomData;

use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::communication::CodecError;

pub struct ControlPlaneCodec<T, U>
where
    T: Serialize,
    U: for<'a> Deserialize<'a>,
{
    msg_size: Option<usize>,
    phantom: PhantomData<(T, U)>,
}

impl<T, U> ControlPlaneCodec<T, U>
where
    T: Serialize,
    U: for<'a> Deserialize<'a>,
{
    pub fn new() -> Self {
        Self {
            msg_size: None,
            phantom: PhantomData,
        }
    }

    fn try_read_message(
        &self,
        buf: &mut BytesMut,
        msg_size: usize,
    ) -> Result<Option<U>, CodecError> {
        if buf.len() >= msg_size {
            let msg_bytes = buf.split_to(msg_size);
            bincode::deserialize(&msg_bytes)
                .map(|msg| Some(msg))
                .map_err(CodecError::from)
        } else {
            Ok(None)
        }
    }

    fn try_read_msg_size(&self, buf: &mut BytesMut) -> Option<usize> {
        if buf.len() >= 4 {
            let msg_size_bytes = buf.split_to(4);
            let msg_size = NetworkEndian::read_u32(&msg_size_bytes);
            Some(msg_size as usize)
        } else {
            None
        }
    }
}

impl<T, U> Decoder for ControlPlaneCodec<T, U>
where
    T: Serialize,
    U: for<'a> Deserialize<'a>,
{
    type Item = U;
    type Error = CodecError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, CodecError> {
        if let Some(msg_size) = self.msg_size {
            // We already have a message size, decode the message.
            self.try_read_message(buf, msg_size)
        } else if let Some(msg_size) = self.try_read_msg_size(buf) {
            // Try to read the message size.
            self.msg_size = Some(msg_size);
            self.try_read_message(buf, msg_size)
        } else {
            // We need more bytes before we can read the message size.
            Ok(None)
        }
    }
}

impl<T, U> Encoder<T> for ControlPlaneCodec<T, U>
where
    T: Serialize,
    U: for<'a> Deserialize<'a>,
{
    type Error = CodecError;

    fn encode(&mut self, msg: T, buf: &mut BytesMut) -> Result<(), CodecError> {
        // Get the serialized size of the message header.
        let msg_size = bincode::serialized_size(&msg).map_err(CodecError::from)? as u32;
        // Write the size of the serialized message.
        let mut size_buffer: Vec<u8> = Vec::new();
        size_buffer.write_u32::<NetworkEndian>(msg_size)?;
        buf.extend(size_buffer);
        // Serialize and write the message.
        let serialized_msg = bincode::serialize(&msg).map_err(CodecError::from)?;
        buf.extend(serialized_msg);
        Ok(())
    }
}
