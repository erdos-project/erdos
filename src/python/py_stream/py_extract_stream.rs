use pyo3::{exceptions, prelude::*};

use crate::{
    dataflow::stream::{errors::TryReadError, ExtractStream, StreamT},
    python::PyMessage,
};

use super::PyStream;

/// The internal Python abstraction over an `ExtractStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.ExtractStream`.
#[pyclass]
pub struct PyExtractStream {
    extract_stream: ExtractStream<Vec<u8>>,
}

#[pymethods]
impl PyExtractStream {
    #[new]
    fn new(py_stream: &PyStream, name: Option<String>) -> Self {
        match name {
            Some(_name) => Self {
                extract_stream: ExtractStream::new_with_name(0, &py_stream.stream, &_name),
            },
            None => Self {
                extract_stream: ExtractStream::new(0, &py_stream.stream),
            },
        }
    }

    fn is_closed(&self) -> bool {
        self.extract_stream.is_closed()
    }

    fn read<'p>(&mut self, py: Python<'p>) -> PyResult<PyMessage> {
        let result = py.allow_threads(|| self.extract_stream.read());
        match result {
            Ok(msg) => Ok(PyMessage::from(msg)),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Unable to to read from stream {}: {:?}",
                self.extract_stream.id(),
                e
            ))),
        }
    }

    fn try_read<'p>(&mut self) -> PyResult<Option<PyMessage>> {
        match self.extract_stream.try_read() {
            Ok(msg) => Ok(Some(PyMessage::from(msg))),
            Err(TryReadError::Empty) => Ok(None),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Unable to to read from stream {}: {:?}",
                self.extract_stream.id(),
                e
            ))),
        }
    }
}
