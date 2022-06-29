use erdos::dataflow::{stream::StreamId, LoopStream, Stream};
use pyo3::prelude::*;

use super::{PyOperatorStream, PyStream};

/// The internal Python abstraction over a `LoopStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.LoopStream`.
#[pyclass(extends=PyStream)]
pub struct PyLoopStream {
    loop_stream: LoopStream<Vec<u8>>,
}

#[pymethods]
impl PyLoopStream {
    #[new]
    fn new() -> (Self, PyStream) {
        let loop_stream = LoopStream::new();
        let id = loop_stream.id();
        (Self { loop_stream }, PyStream { id })
    }

    fn connect_loop(&self, stream: &PyOperatorStream) {
        self.loop_stream.connect_loop(&stream.stream);
    }
}

impl Stream<Vec<u8>> for PyLoopStream {
    fn id(&self) -> StreamId {
        self.loop_stream.id()
    }
    fn name(&self) -> String {
        
    }
}
