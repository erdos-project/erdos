use pyo3::prelude::*;

use crate::dataflow::stream::Stream;

#[pyclass]
pub struct PyStream {
    pub stream: Stream<Vec<u8>>,
}

impl From<Stream<Vec<u8>>> for PyStream {
    fn from(stream: Stream<Vec<u8>>) -> Self {
        Self { stream }
    }
}
