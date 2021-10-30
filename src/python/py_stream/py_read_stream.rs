use pyo3::create_exception;
use pyo3::{exceptions, prelude::*};
use std::sync::Arc;

use crate::{
    dataflow::stream::{
        errors::{ReadError, TryReadError},
        ReadStream,
    },
    python::PyMessage,
};

// Define errors that can be raised by a read stream.
create_exception!(ReadStreamError, SerializationError, exceptions::PyException);
create_exception!(ReadStreamError, Disconnected, exceptions::PyException);
create_exception!(ReadStreamError, Closed, exceptions::PyException);

/// The internal Python abstraction over a `ReadStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.ReadStream`.
#[pyclass]
pub struct PyReadStream {
    pub read_stream: Arc<ReadStream<Vec<u8>>>,
}

#[pymethods]
impl PyReadStream {
    fn is_closed(&self) -> bool {
        self.read_stream.is_closed()
    }

    fn name(&self) -> String {
        self.read_stream.name()
    }

    /// Returns (timestamp, data)
    fn read(&mut self) -> PyResult<PyMessage> {
        // NOTE: Since the executor of a Python's `run` method holds a reference to the same Arc
        // that backs a PyReadStream (in order to drop after its execution), we need to do a
        // `get_mut_unchecked` instead of a `get_mut` to bypass the reference counting checks, and
        // retrieve the underlying ReadStream.
        unsafe {
            let read_stream = Arc::get_mut_unchecked(&mut self.read_stream);
            match read_stream.read() {
                Ok(msg) => Ok(PyMessage::from(msg)),
                Err(e) => {
                    let error_str = format!(
                        "Error reading from {} (ID: {})",
                        self.read_stream.name(),
                        self.read_stream.id()
                    );
                    match e {
                        ReadError::SerializationError => {
                            Err(SerializationError::new_err(error_str))
                        }
                        ReadError::Disconnected => Err(Disconnected::new_err(error_str)),
                        ReadError::Closed => Err(Closed::new_err(error_str)),
                    }
                }
            }
        }
    }

    fn try_read(&mut self) -> PyResult<Option<PyMessage>> {
        // NOTE: Since the executor of a Python's `run` method holds a reference to the same Arc
        // that backs a PyReadStream (in order to drop after its execution), we need to do a
        // `get_mut_unchecked` instead of a `get_mut` to bypass the reference counting checks, and
        // retrieve the underlying ReadStream.
        unsafe {
            let read_stream = Arc::get_mut_unchecked(&mut self.read_stream);
            match read_stream.try_read() {
                Ok(msg) => Ok(Some(PyMessage::from(msg))),
                Err(e) => {
                    let error_str = format!(
                        "Error reading from {} (ID: {})",
                        self.read_stream.name(),
                        self.read_stream.id()
                    );
                    match e {
                        TryReadError::SerializationError => {
                            Err(SerializationError::new_err(error_str))
                        }
                        TryReadError::Disconnected => Err(Disconnected::new_err(error_str)),
                        TryReadError::Closed => Err(Closed::new_err(error_str)),
                        TryReadError::Empty => Ok(None),
                    }
                }
            }
        }
    }
}

impl From<ReadStream<Vec<u8>>> for PyReadStream {
    fn from(read_stream: ReadStream<Vec<u8>>) -> Self {
        Self {
            read_stream: Arc::new(read_stream),
        }
    }
}

impl From<&Arc<ReadStream<Vec<u8>>>> for PyReadStream {
    fn from(read_stream: &Arc<ReadStream<Vec<u8>>>) -> Self {
        Self {
            read_stream: Arc::clone(read_stream),
        }
    }
}
