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
        self.stream.name()
    }

    fn set_name(&mut self, name: String) {
        self.stream.set_name(&name)
    }

    fn id(&self) -> String {
        format!("{}", self.stream.id())
    }
}

// Rust-only methods
impl PyOperatorStream {
    /// Produces a [`PyOperatorStream`] and its [`PyStream`] base class
    /// from a Rust [`OperatorStream`].
    pub(crate) fn new(py: Python, operator_stream: OperatorStream<Vec<u8>>) -> PyResult<Py<Self>> {
        let base_class = PyStream {
            id: operator_stream.id(),
        };
        let initializer =
            PyClassInitializer::from(base_class).add_subclass(Self::from(operator_stream));
        Py::new(py, initializer)
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
