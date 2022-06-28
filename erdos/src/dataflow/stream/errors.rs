use crate::communication::{CommunicationError, TryRecvError};

/// Errors raised when something went wrong while reading from a `ReadStream`.
#[derive(Debug, PartialEq)]
pub enum ReadError {
    /// Message deserialization failed.
    SerializationError,
    /// The channel or the TCP stream has been closed.
    Disconnected,
    /// Stream is closed and can longer sends messages.
    Closed,
}

// TODO (Sukrit) :: Should we deprecate this? We should have a single ReadError that includes
// Empty.
/// Errors raised by calling `try_read` from a `ReadStream`.
#[derive(Debug, PartialEq)]
pub enum TryReadError {
    /// No message available in the buffer.
    Empty,
    /// The channel or the TCP stream has been closed.
    Disconnected,
    /// Message deserialization failed.
    SerializationError,
    /// Stream is closed and can longer sends messages.
    Closed,
}

impl From<TryRecvError> for TryReadError {
    fn from(e: TryRecvError) -> Self {
        match e {
            TryRecvError::Empty => Self::Empty,
            TryRecvError::Disconnected => Self::Disconnected,
            TryRecvError::BincodeError(_) => Self::SerializationError,
        }
    }
}

/// Error raised when something went wrong while sending on a `WriteStream`.
#[derive(Debug, PartialEq)]
pub enum SendError {
    /// Message serialization failed.
    SerializationError,
    /// There was a network or a `mpsc::channel` error.
    IOError,
    /// Timestamp or watermark is smaller or equal to the low watermark.
    TimestampError,
    /// Stream is closed and can no longer send messages.
    Closed,
}

impl From<CommunicationError> for SendError {
    fn from(e: CommunicationError) -> Self {
        match e {
            CommunicationError::NoCapacity | CommunicationError::Disconnected => SendError::IOError,
            CommunicationError::SerializeNotImplemented
            | CommunicationError::DeserializeNotImplemented => {
                eprintln!("Serialize not implemented");
                SendError::SerializationError
            }
            CommunicationError::AbomonationError(error) => {
                eprintln!("Abomonation error {}", error);
                SendError::SerializationError
            }
            CommunicationError::BincodeError(error) => {
                eprintln!("Bincode error {}", error);
                SendError::SerializationError
            }
            CommunicationError::IoError(io_error) => {
                eprintln!("Got write stream IOError {}", io_error);
                SendError::IOError
            }
            // Streams should not be exposed to protocol errors.
            CommunicationError::ProtocolError(_) => unreachable!(),
        }
    }
}
