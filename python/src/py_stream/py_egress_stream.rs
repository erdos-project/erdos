use erdos::dataflow::stream::{errors::TryReadError, EgressStream};
use pyo3::{exceptions, prelude::*};

use crate::PyMessage;

/// The internal Python abstraction over an `EgressStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.EgressStream`.
#[pyclass]
pub struct PyEgressStream {
    egress_stream: EgressStream<Vec<u8>>,
}

#[pymethods]
impl PyEgressStream {
    fn is_closed(&self) -> bool {
        self.egress_stream.is_closed()
    }

    fn read(&mut self, py: Python) -> PyResult<PyMessage> {
        let result = py.allow_threads(|| self.egress_stream.read());
        match result {
            Ok(msg) => Ok(PyMessage::from(msg)),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Unable to to read from stream {}: {:?}",
                self.egress_stream.id(),
                e
            ))),
        }
    }

    fn try_read(&mut self) -> PyResult<Option<PyMessage>> {
        match self.egress_stream.try_read() {
            Ok(msg) => Ok(Some(PyMessage::from(msg))),
            Err(TryReadError::Empty) => Ok(None),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Unable to to read from stream {}: {:?}",
                self.egress_stream.id(),
                e
            ))),
        }
    }

    #[getter(name)]
    fn name(&self) -> String {
        self.egress_stream.name()
    }

    #[getter(id)]
    fn id(&self) -> String {
        format!("{}", self.egress_stream.id())
    }
}

impl PyEgressStream {
    pub(crate) fn new(py: Python, egress_stream: EgressStream<Vec<u8>>) -> PyResult<Py<Self>> {
        let initializer = PyClassInitializer::from(Self::from(egress_stream));
        Py::new(py, initializer)
    }
}

impl From<EgressStream<Vec<u8>>> for PyEgressStream {
    fn from(egress_stream: EgressStream<Vec<u8>>) -> Self {
        Self { egress_stream }
    }
}
