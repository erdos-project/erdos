use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::{BufMut, BytesMut};
use std::fmt::Debug;
use tokio::codec::Decoder;
use tokio::codec::Encoder;

use crate::communication::{CodecError, MessageHeader, SerializedMessage};

/// Encodes messages into bytes, and decodes bytes into SerializedMessages.
///
/// For each message, the codec first writes the size of its message header,
/// then the message header, and finally the content of the message.
#[derive(Debug)]
pub struct MessageCodec {
    header_size: Option<usize>,
    header: Option<MessageHeader>,
}

impl MessageCodec {
    pub fn new() -> MessageCodec {
        MessageCodec {
            header_size: None,
            header: None,
        }
    }

    /// Tries to read the size of the message header from the stream.
    fn try_read_header_size(&self, buf: &mut BytesMut) -> Option<usize> {
        if buf.len() >= 4 {
            let header_size_bytes = buf.split_to(4);
            let header_size = NetworkEndian::read_u32(&header_size_bytes);
            Some(header_size as usize)
        } else {
            None
        }
    }

    /// Tries to read a message header from the stream.
    fn try_read_header(&mut self, buf: &mut BytesMut) -> Result<Option<MessageHeader>, CodecError> {
        let header_size = self.header_size.unwrap();
        if buf.len() >= header_size {
            // Split the buffer where the message ends. The returned
            // buffer will contain the bytes [0, header_size), and
            // the self buffer will contain bytes [header_size..].
            let header_bytes = buf.split_to(header_size);
            // Upon the next invocation we have to read the size
            // of the next message.
            let header: MessageHeader =
                bincode::deserialize(&header_bytes).map_err(|e| CodecError::from(e))?;
            Ok(Some(header))
        } else {
            Ok(None)
        }
    }

    /// Tries to read the message content from the stream.
    fn try_read_message(&mut self, buf: &mut BytesMut) -> Option<SerializedMessage> {
        if let Some(header) = &self.header {
            if buf.len() >= header.data_size {
                // We have sufficient bytes to read the entire message.
                let data_bytes = buf.split_to(header.data_size);
                let msg = SerializedMessage {
                    // Consure the header so that the coded can read the next header.
                    header: self.header.take().unwrap(),
                    data: data_bytes,
                };
                // Reset the header size so that it is read for the next message.
                self.header_size = None;
                return Some(msg);
            }
        }
        None
    }
}

impl Decoder for MessageCodec {
    type Item = SerializedMessage;
    type Error = CodecError;

    /// Decodes a bunch of bytes into a SerializedMessage.
    ///
    /// It first tries to read the header_size, then the header, and finally the
    /// message content.
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<SerializedMessage>, CodecError> {
        if let Some(_) = &self.header {
            // We already have a header. Read the message data.
            Ok(self.try_read_message(buf))
        } else {
            if let Some(_) = self.header_size {
                // We've previously read how many bytes the header has. Try to read the header.
                if let Some(header) = self.try_read_header(buf)? {
                    self.header = Some(header);
                    Ok(self.try_read_message(buf))
                } else {
                    // We need more bytes before we can read the header.
                    Ok(None)
                }
            } else {
                // Try to read the header size.
                if let Some(header_size) = self.try_read_header_size(buf) {
                    self.header_size = Some(header_size);
                    // Try to read the header.
                    if let Some(header) = self.try_read_header(buf)? {
                        self.header = Some(header);
                        // Try to read the message.
                        Ok(self.try_read_message(buf))
                    } else {
                        Ok(None)
                    }
                } else {
                    // We need more bytes before we can read the header size.
                    Ok(None)
                }
            }
        }
    }
}

impl Encoder for MessageCodec {
    type Item = SerializedMessage;
    type Error = CodecError;

    /// Encondes a SerializedMessage into a bunch of bytes.
    ///
    /// It first writes the header_size, then the header, and finally the
    /// serialized message.
    fn encode(&mut self, msg: SerializedMessage, buf: &mut BytesMut) -> Result<(), CodecError> {
        // Serialize and write the header.
        let header = bincode::serialize(&msg.header).map_err(|e| CodecError::from(e))?;
        // Write the size of the serialized header.
        let mut size_buffer: Vec<u8> = Vec::new();
        size_buffer.write_u32::<NetworkEndian>(header.len() as u32)?;
        // Reserve space for header size, header, and message.
        buf.reserve(size_buffer.len() + header.len() + msg.data.len());
        buf.put_slice(&size_buffer[..]);
        buf.put_slice(&header[..]);
        // Write the message data.
        buf.put_slice(&msg.data[..]);
        Ok(())
    }
}

impl Default for MessageCodec {
    fn default() -> Self {
        Self::new()
    }
}
