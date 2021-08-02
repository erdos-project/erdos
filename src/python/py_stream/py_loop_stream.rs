use pyo3::prelude::*;

use crate::dataflow::{LoopStream, ReadStream};

use super::PyReadStream;

#[pyclass]
pub struct PyLoopStream {
    loop_stream: LoopStream<Vec<u8>>,
}

#[pymethods]
impl PyLoopStream {
    #[new]
    fn new(obj: &PyRawObject) {
        obj.init(Self {
            loop_stream: LoopStream::new(),
        });
    }

    fn set(&self, read_stream: &PyReadStream) {
        self.loop_stream.set(&read_stream.read_stream);
    }
}
