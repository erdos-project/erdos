use pyo3::{exceptions, prelude::*};

use crate::{dataflow::stream::ExtractStream, python::PyMessage};

use super::PyReadStream;

#[pyclass]
pub struct PyExtractStream {
    extract_stream: ExtractStream<Vec<u8>>,
}

#[pymethods]
impl PyExtractStream {
    #[new]
    fn new(obj: &PyRawObject, py_read_stream: &PyReadStream, name: Option<String>) {
        let extract_stream = match name {
            Some(_name) => Self {
                extract_stream: ExtractStream::new_with_name(
                    0,
                    &py_read_stream.read_stream,
                    &_name,
                ),
            },
            None => Self {
                extract_stream: ExtractStream::new(0, &py_read_stream.read_stream),
            },
        };
        obj.init(extract_stream);
    }

    fn is_closed(&self) -> bool {
        self.extract_stream.is_closed()
    }

    fn read<'p>(&mut self, py: Python<'p>) -> PyResult<PyMessage> {
        let result = py.allow_threads(|| self.extract_stream.read());
        match result {
            Ok(msg) => Ok(PyMessage::from(msg)),
            Err(e) => Err(exceptions::Exception::py_err(format!(
                "Unable to to read from stream {}: {:?}",
                self.extract_stream.get_id(),
                e
            ))),
        }
    }
}
