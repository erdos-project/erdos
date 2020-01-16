use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::{
    dataflow::{
        stream::{IngestStream, ReadStream, WriteStreamT},
        Message, Timestamp,
    },
    node::NodeId,
};

use super::PyReadStream;

#[pyclass]
pub struct PyIngestStream {
    ingest_stream: IngestStream<Vec<u8>>,
}

#[pymethods]
impl PyIngestStream {
    #[new]
    fn new(obj: &PyRawObject, node_id: NodeId) {
        obj.init(Self {
            ingest_stream: IngestStream::new(node_id),
        });
    }

    fn send(&mut self, _py: Python, timestamp: Vec<u64>, data: &PyBytes) -> PyResult<bool> {
        let timestamp = Timestamp::new(timestamp);
        let data = Vec::from(data.as_bytes());
        match self
            .ingest_stream
            .send(Message::new_message(timestamp, data))
        {
            Ok(_) => Ok(true),
            _ => Ok(false),
        }
    }

    fn send_watermark(&mut self, _py: Python<'_>, timestamp: Vec<u64>) -> PyResult<bool> {
        let timestamp = Timestamp::new(timestamp);
        match self.ingest_stream.send(Message::new_watermark(timestamp)) {
            Ok(_) => Ok(true),
            _ => Ok(false),
        }
    }

    fn to_py_read_stream(&self, _py: Python) -> PyReadStream {
        PyReadStream::from(ReadStream::from(&self.ingest_stream))
    }
}
