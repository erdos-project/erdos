use pyo3::create_exception;
use pyo3::{exceptions, prelude::*, types::PyBytes};

use crate::{
    dataflow::stream::{
        errors::{ReadError, TryReadError},
        ReadStream, StreamId, StreamT,
    },
    python::PyMessage,
};

use super::PyWriteStream;

// Define errors that can be raised by a read stream.
create_exception!(ReadStreamError, SerializationError, exceptions::Exception);
create_exception!(ReadStreamError, Disconnected, exceptions::Exception);
create_exception!(ReadStreamError, Closed, exceptions::Exception);

#[pyclass]
pub struct PyReadStream {
    pub read_stream: ReadStream<Vec<u8>>,
}

#[pymethods]
impl PyReadStream {
    fn is_closed(&self) -> bool {
        self.read_stream.is_closed()
    }

    /// Returns (timestamp, data)
    fn read(&mut self) -> PyResult<PyMessage> {
        match self.read_stream.read() {
            Ok(msg) => Ok(PyMessage::from(msg)),
            Err(e) => {
                let error_str = format!(
                    "Error reading from {} (ID: {})",
                    self.read_stream.name(),
                    self.read_stream.id()
                );
                match e {
                    ReadError::SerializationError => Err(SerializationError::py_err(error_str)),
                    ReadError::Disconnected => Err(Disconnected::py_err(error_str)),
                    ReadError::Closed => Err(Closed::py_err(error_str)),
                }
            }
        }
    }

    fn try_read(&mut self) -> PyResult<Option<PyMessage>> {
        match self.read_stream.try_read() {
            Ok(msg) => Ok(Some(PyMessage::from(msg))),
            Err(e) => {
                let error_str = format!(
                    "Error reading from {} (ID: {})",
                    self.read_stream.name(),
                    self.read_stream.id()
                );
                match e {
                    TryReadError::SerializationError => Err(SerializationError::py_err(error_str)),
                    TryReadError::Disconnected => Err(Disconnected::py_err(error_str)),
                    TryReadError::Closed => Err(Closed::py_err(error_str)),
                    TryReadError::Empty => Ok(None),
                }
            }
        }
    }
}

impl From<ReadStream<Vec<u8>>> for PyReadStream {
    fn from(read_stream: ReadStream<Vec<u8>>) -> Self {
        Self { read_stream }
    }
}
