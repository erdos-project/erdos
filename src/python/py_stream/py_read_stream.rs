use pyo3::create_exception;
use pyo3::{exceptions, prelude::*, types::PyBytes};

use crate::{
    dataflow::stream::errors::{ReadError, TryReadError},
    dataflow::ReadStream,
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
    #[new]
    fn new(obj: &PyRawObject, name: Option<String>) {
        let read_stream = match name {
            Some(_name) => Self {
                read_stream: ReadStream::new_with_name(_name),
            },
            None => Self {
                read_stream: ReadStream::new(),
            },
        };
        obj.init(read_stream);
    }

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
                    self.read_stream.get_name(),
                    self.read_stream.get_id()
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
                    self.read_stream.get_name(),
                    self.read_stream.get_id()
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

    pub fn add_callback(&self, callback: PyObject) {
        self.read_stream.add_callback(move |_timestamp, data| {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let py_bytes = PyBytes::new(py, &data[..]);
            match callback.call1(py, (py_bytes,)) {
                Ok(_) => (),
                Err(e) => e.print(py),
            };
        })
    }

    pub fn add_watermark_callback(&self, callback: PyObject) {
        self.read_stream.add_watermark_callback(move |timestamp| {
            let gil = Python::acquire_gil();
            let py = gil.python();
            match callback.call1(py, (timestamp.time.clone(), timestamp.is_top())) {
                Ok(_) => (),
                Err(e) => e.print(py),
            };
        });
    }
}

impl From<&PyWriteStream> for PyReadStream {
    fn from(py_write_stream: &PyWriteStream) -> Self {
        Self {
            read_stream: ReadStream::from(&py_write_stream.write_stream),
        }
    }
}

impl From<ReadStream<Vec<u8>>> for PyReadStream {
    fn from(read_stream: ReadStream<Vec<u8>>) -> Self {
        Self { read_stream }
    }
}
