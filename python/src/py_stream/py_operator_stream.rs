use pyo3::{exceptions::PyException, prelude::*};

use erdos::dataflow::stream::OperatorStream;

use crate::py_stream::{PyEgressStream, PyStream};

/// The internal Python abstraction over a [`Stream`].
///
/// This class is exposed on the Python interface as `erdos.streams.Stream`.
#[pyclass(extends=PyStream)]
pub struct PyOperatorStream {}

#[pymethods]
impl PyOperatorStream {
    fn to_egress(mut self_: PyRefMut<Self>, py: Python) -> PyResult<Py<PyEgressStream>> {
        if let Some(operator_stream) = self_.as_mut().downcast_stream::<OperatorStream<Vec<u8>>>() {
            PyEgressStream::new(py, operator_stream.to_egress())
        } else {
            Err(PyException::new_err(format!(
                "OperatorStream {}: failed to downcast the PyStream's stream field.",
                self_.as_ref().name()
            )))
        }
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
