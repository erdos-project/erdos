use erdos::dataflow::stream::{OperatorStream, Stream, StreamId};
use pyo3::prelude::*;

use super::PyStream;

/// The internal Python abstraction over a [`Stream`].
///
/// This class is exposed on the Python interface as `erdos.streams.Stream`.
#[pyclass(extends=PyStream)]
pub struct PyOperatorStream {
    pub stream: OperatorStream<Vec<u8>>,
}

#[pymethods]
impl PyOperatorStream {
    fn name(&self) -> String {
        self.name()
    }

    fn set_name(&mut self, name: String) {
        self.set_name(name)
    }

    fn id(&self) -> String {
        format!("{}", self.stream.id())
    }
}

impl Stream<Vec<u8>> for PyOperatorStream {
    fn id(&self) -> StreamId {
        self.stream.id()
    }
}

impl From<OperatorStream<Vec<u8>>> for PyOperatorStream {
    fn from(stream: OperatorStream<Vec<u8>>) -> Self {
        Self { stream }
    }
}

impl From<PyOperatorStream> for OperatorStream<Vec<u8>> {
    fn from(py_stream: PyOperatorStream) -> Self {
        py_stream.stream
    }
}
