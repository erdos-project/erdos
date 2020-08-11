use pyo3::create_exception;
use pyo3::{exceptions, prelude::*};

use crate::{
    dataflow::stream::{errors::WriteStreamError, WriteStreamT},
    dataflow::{Message, WriteStream},
    python::PyMessage,
};

// Define errors that can be raised by a write stream.
create_exception!(WriteStreamError, TimestampError, exceptions::Exception);
create_exception!(WriteStreamError, ClosedError, exceptions::Exception);
create_exception!(WriteStreamError, IOError, exceptions::Exception);
create_exception!(WriteStreamError, SerializationError, exceptions::Exception);

#[pyclass]
pub struct PyWriteStream {
    pub write_stream: WriteStream<Vec<u8>>,
}

#[pymethods]
impl PyWriteStream {
    #[new]
    fn new(obj: &PyRawObject) {
        obj.init(Self {
            write_stream: WriteStream::new(),
        });
    }

    fn is_closed(&self) -> bool {
        self.write_stream.is_closed()
    }

    fn send(&mut self, msg: &PyMessage) -> PyResult<()> {
        self.write_stream.send(Message::from(msg)).map_err(|e| {
            let error_str = format!("Error sending message on {}", self.write_stream.get_id());
            match e {
                WriteStreamError::TimestampError => TimestampError::py_err(error_str),
                WriteStreamError::Closed => ClosedError::py_err(error_str),
                WriteStreamError::IOError => IOError::py_err(error_str),
                WriteStreamError::SerializationError => SerializationError::py_err(error_str),
            }
        })
    }
}

impl From<WriteStream<Vec<u8>>> for PyWriteStream {
    fn from(write_stream: WriteStream<Vec<u8>>) -> Self {
        Self { write_stream }
    }
}
