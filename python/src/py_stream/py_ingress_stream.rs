use pyo3::{exceptions::PyException, prelude::*};

use erdos::dataflow::{stream::IngressStream, Message};

use crate::{PyMessage, PyStream};

/// The internal Python abstraction over an `IngressStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.IngressStream`.
#[pyclass(extends=PyStream)]
pub struct PyIngressStream {}

#[pymethods]
impl PyIngressStream {
    fn is_closed(mut self_: PyRefMut<Self>) -> PyResult<bool> {
        if let Some(ingress_stream) = self_.as_mut().as_mut_stream::<IngressStream<Vec<u8>>>() {
            Ok(ingress_stream.is_closed())
        } else {
            Err(PyException::new_err(format!(
                "IngressStream {}: failed to downcast the PyStream's stream field.",
                self_.as_ref().name()
            )))
        }
    }

    fn send(mut self_: PyRefMut<Self>, msg: &PyMessage) -> PyResult<()> {
        if let Some(ingress_stream) = self_.as_mut().as_mut_stream::<IngressStream<Vec<u8>>>() {
            ingress_stream.send(Message::from(msg)).map_err(|e| {
                PyException::new_err(format!(
                    "IngressStream {}: failed to send the message with {:?}",
                    self_.as_ref().name(),
                    e
                ))
            })
        } else {
            Err(PyException::new_err(format!(
                "IngressStream {}: failed to downcast the PyStream's stream field.",
                self_.as_ref().name()
            )))
        }
    }
}

impl PyIngressStream {
    pub fn new(py: Python, ingress_stream: IngressStream<Vec<u8>>) -> PyResult<Py<Self>> {
        let base_class = PyStream {
            stream: Box::new(ingress_stream),
        };
        let initializer = PyClassInitializer::from(base_class).add_subclass(Self {});
        Py::new(py, initializer)
    }
}
