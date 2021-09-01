use pyo3::{exceptions, prelude::*};

use crate::{
    dataflow::{
        stream::{IngestStream, StreamT},
        Message,
    },
    python::PyMessage,
};

/// The internal Python abstraction over an `IngestStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.IngestStream`.
#[pyclass]
pub struct PyIngestStream {
    ingest_stream: IngestStream<Vec<u8>>,
}

#[pymethods]
impl PyIngestStream {
    #[new]
    fn new(name: Option<String>) -> Self {
        match name {
            Some(_name) => Self {
                ingest_stream: IngestStream::new_with_name(&_name),
            },
            None => Self {
                ingest_stream: IngestStream::new(),
            },
        }
    }

    fn is_closed(&self) -> bool {
        self.ingest_stream.is_closed()
    }

    fn send(&mut self, msg: &PyMessage) -> PyResult<()> {
        self.ingest_stream.send(Message::from(msg)).map_err(|e| {
            exceptions::PyException::new_err(format!(
                "Error sending message on ingest stream {}: {:?}",
                self.ingest_stream.id(),
                e
            ))
        })
    }
}
