use pyo3::prelude::*;

use crate::dataflow::stream::Stream;

/// The internal Python abstraction over an `Stream`.
///
/// This class is exposed on the Python interface as `erdos.streams.Stream`.
#[pyclass]
pub struct PyStream {
    pub stream: Stream<Vec<u8>>,
}

#[pymethods]
impl PyStream {
    fn name(&self) -> String {
        self.stream.name()
    }

    fn set_name(&mut self, name: String) {
        self.stream.set_name(&name)
    }
}

impl From<Stream<Vec<u8>>> for PyStream {
    fn from(stream: Stream<Vec<u8>>) -> Self {
        Self { stream }
    }
}
