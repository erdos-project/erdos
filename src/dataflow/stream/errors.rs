use crate::communication::CommunicationError;

/// Error raised by the WriteStream layer.
#[derive(Debug)]
pub enum WriteStreamError {
    /// Message serialization failed.
    SerializationError,
    /// There was a network or a `mpsc::channel` error.
    IOError,
    /// Timestamp or watermark is smaller or equal to the low watermark.
    TimestampError,
}

impl From<CommunicationError> for WriteStreamError {
    fn from(e: CommunicationError) -> Self {
        match e {
            CommunicationError::NoCapacity | CommunicationError::Disconnected => {
                WriteStreamError::IOError
            }
            CommunicationError::SerializeNotImplemented
            | CommunicationError::DeserializeNotImplemented => {
                eprintln!("Serialize not implemented");
                WriteStreamError::SerializationError
            }
            CommunicationError::AbomonationError(error) => {
                eprintln!("Abomonation error {}", error);
                WriteStreamError::SerializationError
            }
            CommunicationError::BincodeError(error) => {
                eprintln!("Bincode error {}", error);
                WriteStreamError::SerializationError
            }
            CommunicationError::IoError(io_error) => {
                eprintln!("Got write stream IOError {}", io_error);
                WriteStreamError::IOError
            }
        }
    }
}
