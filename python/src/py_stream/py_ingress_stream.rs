use erdos::dataflow::{
    stream::IngressStream,
    Message, Stream,
};
use pyo3::{exceptions, prelude::*};

use crate::{PyMessage, PyStream};

/// The internal Python abstraction over an `IngressStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.IngressStream`.
#[pyclass(extends=PyStream)]
pub struct PyIngressStream {
    pub ingress_stream: IngressStream<Vec<u8>>,
}

#[pymethods]
impl PyIngressStream {
    fn is_closed(&self) -> bool {
        self.ingress_stream.is_closed()
    }

    fn send(&mut self, msg: &PyMessage) -> PyResult<()> {
        self.ingress_stream.send(Message::from(msg)).map_err(|e| {
            exceptions::PyException::new_err(format!(
                "Error sending message on ingress stream {}: {:?}",
                self.ingress_stream.id(),
                e
            ))
        })
    }

    #[getter(name)]
    fn name(&self) -> String {
        self.ingress_stream.name()
    }

    #[getter(id)]
    fn id(&self) -> String {
        format!("{}", self.ingress_stream.id())
    }
}

impl PyIngressStream {
    pub(crate) fn new(py: Python, ingress_stream: IngressStream<Vec<u8>>) -> PyResult<Py<Self>> {
        let base_class = PyStream {
            id: ingress_stream.id(),
            name: ingress_stream.name(),
            graph: ingress_stream.graph(),
        };
        let initializer =
            PyClassInitializer::from(base_class).add_subclass(Self::from(ingress_stream));
        Py::new(py, initializer)
    }
}

impl From<IngressStream<Vec<u8>>> for PyIngressStream {
    fn from(ingress_stream: IngressStream<Vec<u8>>) -> Self {
        Self { ingress_stream }
    }
}
