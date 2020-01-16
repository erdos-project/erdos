use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::dataflow::{stream::WriteStreamT, Message, Timestamp, WriteStream};

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

    fn send(&mut self, _py: Python<'_>, timestamp: Vec<u64>, data: &PyBytes) -> PyResult<bool> {
        let timestamp = Timestamp::new(timestamp);
        let data = Vec::from(data.as_bytes());
        match self
            .write_stream
            .send(Message::new_message(timestamp, data))
        {
            Ok(_) => Ok(true),
            _ => Ok(false),
        }
    }

    fn send_watermark(&mut self, _py: Python<'_>, timestamp: Vec<u64>) -> PyResult<bool> {
        let timestamp = Timestamp::new(timestamp);
        match self.write_stream.send(Message::new_watermark(timestamp)) {
            Ok(_) => Ok(true),
            _ => Ok(false),
        }
    }
}

impl From<WriteStream<Vec<u8>>> for PyWriteStream {
    fn from(write_stream: WriteStream<Vec<u8>>) -> Self {
        Self { write_stream }
    }
}
