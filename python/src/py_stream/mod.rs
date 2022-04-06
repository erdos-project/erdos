use erdos::dataflow::stream::{Stream, StreamId};
use pyo3::prelude::*;

// Private submodules
mod py_extract_stream;
mod py_ingest_stream;
mod py_loop_stream;
mod py_operator_stream;
mod py_read_stream;
mod py_write_stream;

// Public exports
pub use py_extract_stream::PyExtractStream;
pub use py_ingest_stream::PyIngestStream;
pub use py_loop_stream::PyLoopStream;
pub use py_operator_stream::PyOperatorStream;
pub use py_read_stream::PyReadStream;
pub use py_write_stream::PyWriteStream;

/// The internal Python abstraction over a [`Stream`].
#[pyclass(subclass)]
pub struct PyStream {
    pub id: StreamId,
}

#[pymethods]
impl PyStream {
    fn name(&self) -> String {
        Stream::name(self)
    }

    fn set_name(&mut self, name: &str) {
        Stream::set_name(self, name)
    }

    fn id(&self) -> String {
        format!("{}", self.id)
    }
}

impl Stream<Vec<u8>> for PyStream {
    fn id(&self) -> StreamId {
        self.id
    }
}
