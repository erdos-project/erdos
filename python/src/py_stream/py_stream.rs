use erdos::dataflow::stream::{OperatorStream, Stream, StreamId};
use pyo3::prelude::*;

/// The internal Python abstraction over a [`Stream`].
#[pyclass(subclass)]
pub struct PyStream {
    pub id: StreamId,
}

#[pymethods]
impl PyStream {
    fn name(&self) -> String {
        self.name()
    }

    fn set_name(&mut self, name: String) {
        self.set_name(name)
    }

    fn id(&self) -> String {
        format!("{}", self.id)
    }
}

impl Stream<Vec<u8>> for PyStream {
    fn id(&self) -> StreamId {
        self.id
    }
}
