use erdos::dataflow::LoopStream;
use pyo3::prelude::*;

use super::PyOperatorStream;

/// The internal Python abstraction over a `LoopStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.LoopStream`.
#[pyclass]
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
        let initializer = PyClassInitializer::from(Self::from(loop_stream));
        Py::new(py, initializer)
    }
}

impl From<LoopStream<Vec<u8>>> for PyLoopStream {
    fn from(loop_stream: LoopStream<Vec<u8>>) -> Self {
        Self { loop_stream }
    }
}
