use pyo3::prelude::*;

use crate::dataflow::LoopStream;

use super::PyStream;

/// The internal Python abstraction over a `LoopStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.LoopStream`.
#[pyclass]
pub struct PyLoopStream {
    loop_stream: LoopStream<Vec<u8>>,
}

#[pymethods]
impl PyLoopStream {
    #[new]
    fn new() -> Self {
        Self {
            loop_stream: LoopStream::new(),
        }
    }

    fn set(&self, stream: &PyStream) {
        self.loop_stream.set(&stream.stream);
    }
}
