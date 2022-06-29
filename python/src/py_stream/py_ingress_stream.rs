use erdos::dataflow::{stream::IngressStream, Message, Stream};
use pyo3::{exceptions, prelude::*};

use crate::{PyMessage, PyStream};

/// The internal Python abstraction over an `IngestStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.IngestStream`.
#[pyclass(extends=PyStream)]
pub struct PyIngestStream {
    ingest_stream: IngestStream<Vec<u8>>,
}

#[pymethods]
impl PyIngestStream {
    #[new]
    fn new(name: Option<String>) -> (Self, PyStream) {
        let mut ingest_stream = IngestStream::new();
        if let Some(name_str) = name {
            ingest_stream.set_name(&name_str);
        }

        let id = ingest_stream.id();
        (Self { ingest_stream }, PyStream { id })
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

    fn name(&self) -> String {
        self.ingest_stream.name()
    }

    fn set_name(&mut self, name: String) {
        self.ingest_stream.set_name(&name)
    }

    fn id(&self) -> String {
        format!("{}", self.ingest_stream.id())
    }
}
