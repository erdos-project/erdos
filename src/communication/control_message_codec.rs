use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::BytesMut;
use std::fmt::Debug;
use tokio::codec::Decoder;
use tokio::codec::Encoder;

use crate::communication::{CodecError, ControlMessage};

#[derive(Debug)]
pub struct ControlMessageCodec {
    msg_size: Option<usize>,
}

impl ControlMessageCodec {
    pub fn new() -> ControlMessageCodec {
        ControlMessageCodec { msg_size: None }
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

    fn try_read_message(&mut self, buf: &mut BytesMut) -> Option<ControlMessage> {
        let msg_size = self.msg_size.unwrap();
        if buf.len() >= msg_size {
            let msg_bytes = buf.split_to(msg_size);
            let msg = bincode::deserialize(&msg_bytes)
                .map_err(|e| CodecError::from(e))
                .unwrap();
            self.msg_size = None;
            Some(msg)
        } else {
            None
        }
    }
}

impl Decoder for ControlMessageCodec {
    type Item = ControlMessage;
    type Error = CodecError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<ControlMessage>, CodecError> {
        if let Some(_) = self.msg_size {
            // We already have a message size.
            Ok(self.try_read_message(buf))
        } else {
            // Try to read the message size.
            if let Some(msg_size) = self.try_read_msg_size(buf) {
                self.msg_size = Some(msg_size);
                Ok(self.try_read_message(buf))
            } else {
                // We need more bytes before we can read the message size.
                Ok(None)
            }
        }
    }
}

impl Encoder for ControlMessageCodec {
    type Item = ControlMessage;
    type Error = CodecError;

    fn encode(&mut self, msg: ControlMessage, buf: &mut BytesMut) -> Result<(), CodecError> {
        // Get the serialized size of the message header.
        let msg_size = bincode::serialized_size(&msg).map_err(|e| CodecError::from(e))? as u32;
        // Write the size of the serialized message.
        let mut size_buffer: Vec<u8> = Vec::new();
        size_buffer.write_u32::<NetworkEndian>(msg_size)?;
        buf.extend(size_buffer);
        // Serialize and write the message.
        let serialized_msg = bincode::serialize(&msg).map_err(|e| CodecError::from(e))?;
        buf.extend(serialized_msg);
        Ok(())
    }
}

impl Default for ControlMessageCodec {
    fn default() -> Self {
        Self::new()
    }
}
