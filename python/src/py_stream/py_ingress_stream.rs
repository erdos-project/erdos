use erdos::dataflow::{stream::IngressStream, Message, Stream};
use pyo3::{exceptions, prelude::*};

use crate::{PyMessage, PyStream};

/// The internal Python abstraction over an `IngressStream`.
///
/// This class is exposed on the Python interface as `erdos.streams.IngressStream`.
#[pyclass(extends=PyStream)]
pub struct PyIngressStream {}

#[pymethods]
impl PyIngressStream {
    fn is_closed(self_: PyRef<Self>) -> bool {
        unimplemented!();
        let super_ = self_.as_ref();

        // self.ingress_stream.is_closed()
    }

    fn send(&mut self, msg: &PyMessage) -> PyResult<()> {
        unimplemented!();
        // self.ingress_stream.send(Message::from(msg)).map_err(|e| {
        //     exceptions::PyException::new_err(format!(
        //         "Error sending message on ingress stream {}: {:?}",
        //         self.ingress_stream.id(),
        //         e
        //     ))
        // })
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
