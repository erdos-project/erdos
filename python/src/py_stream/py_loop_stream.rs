use erdos::dataflow::{LoopStream, Stream};
use pyo3::prelude::*;

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

    fn connect_loop(&self, stream: &PyStream) {
        self.loop_stream.connect_loop(&stream.stream);
    }

    fn to_py_stream(&self) -> PyStream {
        PyStream {
            stream: Stream::from(&self.loop_stream),
        }
    }
}
