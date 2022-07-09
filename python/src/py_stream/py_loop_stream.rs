use erdos::dataflow::LoopStream;
use pyo3::prelude::*;

use crate::py_stream::PyStream;

use super::PyOperatorStream;

/// The internal Python abstraction over a `LoopStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.LoopStream`.
#[pyclass(extends=PyStream)]
pub struct PyLoopStream {}

#[pymethods]
impl PyLoopStream {
    fn connect_loop(&mut self, stream: &PyOperatorStream) {
        unimplemented!();
        // self.loop_stream.connect_loop(&stream.stream);
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
