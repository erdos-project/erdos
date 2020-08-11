use pyo3::{exceptions, prelude::*};

use crate::{
    dataflow::{
        stream::{IngestStream, ReadStream},
        Message,
    },
    node::NodeId,
    python::PyMessage,
};

use super::PyReadStream;

#[pyclass]
pub struct PyIngestStream {
    ingest_stream: IngestStream<Vec<u8>>,
}

#[pymethods]
impl PyIngestStream {
    #[new]
    fn new(obj: &PyRawObject, node_id: NodeId, name: Option<String>) {
        let ingest_stream = match name {
            Some(_name) => Self {
                ingest_stream: IngestStream::new_with_name(node_id, &_name),
            },
            None => Self {
                ingest_stream: IngestStream::new(node_id),
            },
        };
        obj.init(ingest_stream);
    }

    fn is_closed(&self) -> bool {
        self.ingest_stream.is_closed()
    }

    fn send(&mut self, msg: &PyMessage) -> PyResult<()> {
        self.ingest_stream.send(Message::from(msg)).map_err(|e| {
            exceptions::Exception::py_err(format!(
                "Error sending message on ingest stream {}: {:?}",
                self.ingest_stream.get_id(),
                e
            ))
        })
    }

    fn to_py_read_stream(&self) -> PyReadStream {
        PyReadStream::from(ReadStream::from(&self.ingest_stream))
    }
}
