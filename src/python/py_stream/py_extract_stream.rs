use pyo3::{exceptions, prelude::*, types::PyBytes};

use crate::dataflow::{stream::ExtractStream, Message};

use super::PyReadStream;

#[pyclass]
pub struct PyExtractStream {
    extract_stream: ExtractStream<Vec<u8>>,
}

#[pymethods]
impl PyExtractStream {
    #[new]
    fn new(obj: &PyRawObject, py_read_stream: &PyReadStream) {
        obj.init(Self {
            extract_stream: ExtractStream::new(0, &py_read_stream.read_stream),
        });
    }

    fn read<'p>(&mut self, py: Python<'p>) -> PyResult<(Vec<u64>, Option<&'p PyBytes>)> {
        let result = py.allow_threads(|| self.extract_stream.read());
        match result {
            Some(Message::TimestampedData(msg)) => {
                Ok((msg.timestamp.time, Some(PyBytes::new(py, &msg.data[..]))))
            }
            Some(Message::Watermark(timestamp)) => Ok((timestamp.time, None)),
            None => Err(PyErr::new::<exceptions::Exception, _>(
                "Unable to to read from stream",
            )),
        }
    }
}
