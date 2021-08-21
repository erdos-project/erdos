use pyo3::create_exception;
use pyo3::{exceptions, prelude::*};

use crate::{
    dataflow::stream::{errors::WriteStreamError, StreamT, WriteStreamT},
    dataflow::{Message, WriteStream},
    python::PyMessage,
};

// Define errors that can be raised by a write stream.
create_exception!(WriteStreamError, TimestampError, exceptions::PyException);
create_exception!(WriteStreamError, ClosedError, exceptions::PyException);
create_exception!(WriteStreamError, IOError, exceptions::PyException);
create_exception!(
    WriteStreamError,
    SerializationError,
    exceptions::PyException
);

/// The internal Python abstraction over a `WriteStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.WriteStream`.
#[pyclass]
pub struct PyWriteStream {
    pub write_stream: WriteStream<Vec<u8>>,
}

#[pymethods]
impl PyWriteStream {
    fn is_closed(&self) -> bool {
        self.write_stream.is_closed()
    }

    fn send(&mut self, msg: &PyMessage) -> PyResult<()> {
        self.write_stream.send(Message::from(msg)).map_err(|e| {
            let error_str = format!("Error sending message on {}", self.write_stream.id());
            match e {
                WriteStreamError::TimestampError => TimestampError::new_err(error_str),
                WriteStreamError::Closed => ClosedError::new_err(error_str),
                WriteStreamError::IOError => IOError::new_err(error_str),
                WriteStreamError::SerializationError => SerializationError::new_err(error_str),
            }
        })
    }
}

impl From<WriteStream<Vec<u8>>> for PyWriteStream {
    fn from(write_stream: WriteStream<Vec<u8>>) -> Self {
        Self { write_stream }
    }
}
