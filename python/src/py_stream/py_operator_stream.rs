use erdos::dataflow::stream::{OperatorStream, Stream};
use pyo3::prelude::*;

use super::{PyEgressStream, PyStream};

/// The internal Python abstraction over a [`Stream`].
///
/// This class is exposed on the Python interface as `erdos.streams.Stream`.
#[pyclass(extends=PyStream)]
pub struct PyOperatorStream {}

#[pymethods]
impl PyOperatorStream {
    fn name(&self) -> String {
        // self.stream.name()
        unimplemented!()
    }

    fn id(&self) -> String {
        // format!("{}", self.stream.id())
        unimplemented!()
    }

    fn to_egress(&self, py: Python) -> Py<PyEgressStream> {
        unimplemented!()
        // PyEgressStream::new(py, self.stream.to_egress()).unwrap()
    }
}

// Rust-only methods
impl PyOperatorStream {
    /// Produces a [`PyOperatorStream`] and its [`PyStream`] base class
    /// from a Rust [`OperatorStream`].
    pub(crate) fn new(py: Python, operator_stream: OperatorStream<Vec<u8>>) -> PyResult<Py<Self>> {
        let base_class = PyStream {
            stream: Box::new(operator_stream),
        };
        let initializer = PyClassInitializer::from(base_class).add_subclass(Self {});
        Py::new(py, initializer)
    }
}
