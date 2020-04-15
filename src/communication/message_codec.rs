use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::{BufMut, BytesMut};
use std::fmt::Debug;
use tokio_util::codec::{Decoder, Encoder};

use crate::communication::{CodecError, MessageMetadata, SerializedMessage};

const HEADER_SIZE: usize = 8;

#[derive(Debug)]
enum DecodeStatus {
    Header,
    Metadata {
        metadata_size: usize,
        data_size: usize,
    },
    Data {
        data_size: usize,
    },
}

/// Encodes messages into bytes, and decodes bytes into SerializedMessages.
///
/// For each message, the codec first writes the size of its message header,
/// then the message header, and finally the content of the message.
#[derive(Debug)]
pub struct MessageCodec {
    status: DecodeStatus,
    msg_metadata: Option<MessageMetadata>,
    start: u128,
    num_calls: usize,
}

impl MessageCodec {
    pub fn new() -> MessageCodec {
        MessageCodec {
            status: DecodeStatus::Header,
            msg_metadata: None,
            start: 0,
            num_calls: 0,
        }
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
        self.num_calls += 1;
        match self.status {
            DecodeStatus::Header => {
                self.start = crate::current_time_us();
                if buf.len() >= HEADER_SIZE {
                    let header = buf.split_to(HEADER_SIZE);
                    let metadata_size = NetworkEndian::read_u32(&header[0..4]) as usize;
                    let data_size = NetworkEndian::read_u32(&header[4..8]) as usize;
                    self.status = DecodeStatus::Metadata {
                        metadata_size,
                        data_size,
                    };
                    // Reserve space in the buffer for the rest of the message and the next header.
                    println!(
                        "metadata_size = {}, data_size = {}",
                        metadata_size, data_size,
                    );
                    // buf.reserve(metadata_size + data_size + HEADER_SIZE);
                    self.decode(buf)
                } else {
                    Ok(None)
                }
            }
            DecodeStatus::Metadata {
                metadata_size,
                data_size,
            } => {
                if buf.len() >= metadata_size {
                    let metadata_bytes = buf.split_to(metadata_size);
                    let metadata: MessageMetadata =
                        bincode::deserialize(&metadata_bytes).map_err(CodecError::BincodeError)?;
                    self.msg_metadata = Some(metadata);
                    self.status = DecodeStatus::Data { data_size };
                    self.decode(buf)
                } else {
                    Ok(None)
                }
            }
            DecodeStatus::Data { data_size } => {
                if buf.len() >= data_size {
                    let data = buf.split_to(data_size);
                    self.status = DecodeStatus::Header;
                    let msg = SerializedMessage {
                        metadata: self.msg_metadata.take().unwrap(),
                        data,
                    };

                    let duration_us = crate::current_time_us() - self.start;
                    println!("decoder: {}", self.start);
                    println!(
                        "decoding took {} us and {} calls",
                        duration_us, self.num_calls
                    );
                    self.num_calls = 0;
                    buf.reserve(20_000_000);

                    Ok(Some(msg))
                } else {
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
        let start = crate::current_time_us();
        // Serialize and write the header.
        let metadata = bincode::serialize(&msg.metadata).map_err(CodecError::from)?;
        // Write the size of the serialized header.
        let mut header: Vec<u8> = Vec::with_capacity(HEADER_SIZE);
        header.write_u32::<NetworkEndian>(metadata.len() as u32)?;
        header.write_u32::<NetworkEndian>(msg.data.len() as u32)?;
        // Reserve space for header size, header, and message.
        // buf.reserve(HEADER_SIZE + metadata.len() + msg.data.len());
        buf.put_slice(&header[..]);
        buf.put_slice(&metadata[..]);
        // Write the message data.
        let start_x = crate::current_time_us();
        buf.put_slice(&msg.data[..]);
        println!(
            "putting data in slice took {} us",
            crate::current_time_us() - start_x
        );
        let duration_us = crate::current_time_us() - start;
        println!("encoder: {}", start);
        println!("encoding took {} us", duration_us);
        // TODO: pre-allocate a constant size to speed up
        buf.reserve(20_000_000);
        Ok(())
    }
}

impl Default for MessageCodec {
    fn default() -> Self {
        Self::new()
    }
}
