use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::{buf::ext::BufMutExt, BufMut, BytesMut};
use std::fmt::Debug;
use tokio_util::codec::{Decoder, Encoder};

use crate::communication::{CodecError, InterProcessMessage, MessageMetadata};

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

/// Encodes messages into bytes, and decodes bytes into InterProcessMessages.
///
/// For each message, the codec first writes the size of its message header,
/// then the message header, and finally the content of the message.
#[derive(Debug)]
pub struct MessageCodec {
    status: DecodeStatus,
    msg_metadata: Option<MessageMetadata>,
}

impl MessageCodec {
    pub fn new() -> MessageCodec {
        MessageCodec {
            status: DecodeStatus::Header,
            msg_metadata: None,
        }
    }
}

impl Decoder for MessageCodec {
    type Item = InterProcessMessage;
    type Error = CodecError;

    /// Decodes a bunch of bytes into a InterProcessMessage.
    ///
    /// It first tries to read the header_size, then the header, and finally the
    /// message content.
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<InterProcessMessage>, CodecError> {
        match self.status {
            DecodeStatus::Header => {
                if buf.len() >= HEADER_SIZE {
                    let header = buf.split_to(HEADER_SIZE);
                    let metadata_size = NetworkEndian::read_u32(&header[0..4]) as usize;
                    let data_size = NetworkEndian::read_u32(&header[4..8]) as usize;
                    self.status = DecodeStatus::Metadata {
                        metadata_size,
                        data_size,
                    };
                    // Reserve space in the buffer for the rest of the message and the next header.
                    buf.reserve(metadata_size + data_size + HEADER_SIZE);
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
                    let bytes = buf.split_to(data_size);
                    self.status = DecodeStatus::Header;
                    let byte_size = bytes.len();
                    let msg = InterProcessMessage::new_serialized(
                        bytes,
                        self.msg_metadata.take().unwrap(),
                    );
                    // buf.reserve(20_000_000);
                    Ok(Some(msg))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

impl Encoder for MessageCodec {
    type Item = InterProcessMessage;
    type Error = CodecError;

    /// Encondes a InterProcessMessage into a bunch of bytes.
    ///
    /// It first writes the header_size, then the header, and finally the
    /// serialized message.
    fn encode(&mut self, msg: InterProcessMessage, buf: &mut BytesMut) -> Result<(), CodecError> {
        // Serialize and write the header.
        let (metadata, data, send_us) = match msg {
            InterProcessMessage::Deserialized {
                metadata,
                data,
                send_us,
            } => (metadata, data, send_us),
            InterProcessMessage::Serialized {
                metadata,
                bytes,
                decode_us,
            } => unreachable!(),
        };

        let metadata_size = bincode::serialized_size(&metadata).map_err(CodecError::from)?;
        let data_size = data.serialized_size().unwrap();

        buf.reserve(HEADER_SIZE + metadata_size as usize + data_size);

        let mut writer = buf.writer();

        // Serialize directly into the buffer.
        writer.write_u32::<NetworkEndian>(metadata_size as u32)?;
        writer.write_u32::<NetworkEndian>(data_size as u32)?;

        bincode::serialize_into(&mut writer, &metadata).map_err(CodecError::from)?;
        data.encode_into(buf).unwrap();


        // Pre-allocate a constant size to speed up.
        // TODO: make this configurable.
        /*
        let reserve_start = crate::current_time_us();
        buf.reserve(20_000_000);
        let reserve_duration = crate::current_time_us() - reserve_start;
        */
        
        Ok(())
    }
}

impl Default for MessageCodec {
    fn default() -> Self {
        Self::new()
    }
}
