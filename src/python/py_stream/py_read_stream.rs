use pyo3::{exceptions, prelude::*, types::PyBytes};

use crate::{
    dataflow::{stream::errors::TryReadError, ReadStream},
    python::PyMessage,
};

use super::PyWriteStream;

#[pyclass]
pub struct PyReadStream {
    pub read_stream: ReadStream<Vec<u8>>,
}

#[pymethods]
impl PyReadStream {
    #[new]
    fn new(obj: &PyRawObject) {
        obj.init(Self {
            read_stream: ReadStream::new(),
        });
    }

    fn is_closed(&self) -> bool {
        self.read_stream.is_closed()
    }

    /// Returns (timestamp, data)
    fn read(&mut self) -> PyResult<PyMessage> {
        match self.read_stream.read() {
            Ok(msg) => Ok(PyMessage::from(msg)),
            Err(e) => Err(exceptions::Exception::py_err(format!(
                "Unable to to read from stream {}: {:?}",
                self.read_stream.get_id(),
                e
            ))),
        }
    }

    fn try_read(&mut self) -> PyResult<Option<PyMessage>> {
        match self.read_stream.try_read() {
            Ok(msg) => Ok(Some(PyMessage::from(msg))),
            Err(TryReadError::Empty) => Ok(None),
            Err(e) => Err(exceptions::Exception::py_err(format!(
                "Unable to to read from stream {}: {:?}",
                self.read_stream.get_id(),
                e
            ))),
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
