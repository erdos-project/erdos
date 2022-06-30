use erdos::dataflow::LoopStream;
use pyo3::prelude::*;

use crate::py_stream::PyStream;

use super::PyOperatorStream;

/// The internal Python abstraction over a `LoopStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.LoopStream`.
#[pyclass(extends=PyStream)]
pub struct PyLoopStream {
    loop_stream: LoopStream<Vec<u8>>,
}

#[pymethods]
impl PyLoopStream {
    fn connect_loop(&mut self, stream: &PyOperatorStream) {
        self.loop_stream.connect_loop(&stream.stream);
    }
}

impl PyLoopStream {
    pub(crate) fn new(py: Python, loop_stream: LoopStream<Vec<u8>>) -> PyResult<Py<Self>> {
        let base_class = PyStream {
            id: loop_stream.id(),
            name: format!("LoopStream-{}", loop_stream.id()),
            graph: loop_stream.graph(),
        };
        let initializer =
            PyClassInitializer::from(base_class).add_subclass(Self::from(loop_stream));
        Py::new(py, initializer)
    }
}

impl From<LoopStream<Vec<u8>>> for PyLoopStream {
    fn from(loop_stream: LoopStream<Vec<u8>>) -> Self {
        Self { loop_stream }
    }
}
