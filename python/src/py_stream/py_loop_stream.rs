use pyo3::{exceptions::PyException, prelude::*};

use erdos::dataflow::{stream::OperatorStream, LoopStream};

use crate::py_stream::{PyOperatorStream, PyStream};

/// The internal Python abstraction over a `LoopStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.LoopStream`.
#[pyclass(extends=PyStream)]
pub struct PyLoopStream {}

#[pymethods]
impl PyLoopStream {
    fn connect_loop(
        mut self_: PyRefMut<Self>,
        mut py_operator_stream: PyRefMut<PyOperatorStream>,
    ) -> PyResult<()> {
        if let Some(loop_stream) = self_.as_mut().as_mut_stream::<LoopStream<Vec<u8>>>() {
            if let Some(operator_stream) = py_operator_stream
                .as_mut()
                .as_mut_stream::<OperatorStream<Vec<u8>>>()
            {
                loop_stream.connect_loop(operator_stream);
                Ok(())
            } else {
                Err(PyException::new_err(format!(
                    "OperatorStream {}: failed to downcast the PyStream's stream field.",
                    py_operator_stream.as_ref().name()
                )))
            }
        } else {
            Err(PyException::new_err(format!(
                "LoopStream {}: failed to downcast the PyStream's stream field.",
                self_.as_ref().name()
            )))
        }
    }
}

impl PyLoopStream {
    pub fn new(py: Python, loop_stream: LoopStream<Vec<u8>>) -> PyResult<Py<Self>> {
        let base_class = PyStream {
            stream: Box::new(loop_stream),
        };
        let initializer = PyClassInitializer::from(base_class).add_subclass(Self {});
        Py::new(py, initializer)
    }
}
