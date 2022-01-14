use std::io;
use tokio::sync::mpsc;

/// Error raised by the communication layer.
#[derive(Debug)]
pub enum CommunicationError {
    /// The channel has no capacity left.
    NoCapacity,
    /// The channel or the TCP stream has been closed.
    Disconnected,
    /// Type does not support serialization.
    SerializeNotImplemented,
    /// Type does not support deserialization.
    DeserializeNotImplemented,
    /// Failed to serialize/deserialize data with Abomonation.
    AbomonationError(io::Error),
    /// Failed to serialize/deserialize data with Bincode.
    BincodeError(bincode::Error),
    /// Failed to read/write data from/to the TCP stream.
    IoError(io::Error),
}

impl From<bincode::Error> for CommunicationError {
    fn from(e: bincode::Error) -> Self {
        CommunicationError::BincodeError(e)
    }
}

impl From<io::Error> for CommunicationError {
    fn from(e: io::Error) -> Self {
        CommunicationError::IoError(e)
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for CommunicationError {
    fn from(_e: std::sync::mpsc::SendError<T>) -> Self {
        CommunicationError::Disconnected
    }
}

impl<T> From<mpsc::error::SendError<T>> for CommunicationError {
    fn from(_e: mpsc::error::SendError<T>) -> Self {
        CommunicationError::Disconnected
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for CommunicationError {
    fn from(e: mpsc::error::TrySendError<T>) -> Self {
        match e {
            mpsc::error::TrySendError::Closed(_) => CommunicationError::Disconnected,
            mpsc::error::TrySendError::Full(_) => CommunicationError::NoCapacity,
        }
    }
}

impl From<CodecError> for CommunicationError {
    fn from(e: CodecError) -> Self {
        match e {
            CodecError::IoError(e) => CommunicationError::IoError(e),
            CodecError::BincodeError(e) => CommunicationError::BincodeError(e),
        }
    }
}

/// Error that is raised by the `MessageCodec` or `ControlMessageCodec` when messages cannot
/// be encoded or decoded.
#[derive(Debug)]
pub enum CodecError {
    IoError(io::Error),
    /// Bincode serialization/deserialization error. It is raised when the `MessageMetadata` serialization
    /// fails. This should not ever happen.
    BincodeError(bincode::Error),
}

impl From<io::Error> for CodecError {
    fn from(e: io::Error) -> CodecError {
        CodecError::IoError(e)
    }
}

impl From<bincode::Error> for CodecError {
    fn from(e: bincode::Error) -> Self {
        CodecError::BincodeError(e)
    }
}

#[derive(Debug)]
pub enum TryRecvError {
    /// No data to read.
    Empty,
    /// The channel or the TCP stream has been closed.
    Disconnected,
    /// Failed to serialize/deserialize data.
    BincodeError(bincode::Error),
}
