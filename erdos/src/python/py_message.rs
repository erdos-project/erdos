use pyo3::{exceptions, prelude::*, types::PyBytes};
use std::sync::Arc;

use crate::{dataflow::Message, python::PyTimestamp};

/// The Python version of an ERDOS message.
///
/// This class provides the API that is wrapped around by `erdos.Message` in Python.
#[pyclass]
pub(crate) struct PyMessage {
    msg: Message<Vec<u8>>,
}

#[pymethods]
impl PyMessage {
    #[new]
    fn new<'a>(timestamp: Option<PyTimestamp>, data: Option<&'a PyBytes>) -> PyResult<Self> {
        if timestamp.is_none() && data.is_some() {
            return Err(exceptions::PyValueError::new_err(
                "Passing a non-None value to data when timestamp=None is not allowed",
            ));
        }
        let msg = match (timestamp, data) {
            (Some(t), Some(d)) => Message::new_message(t.into(), Vec::from(d.as_bytes())),
            (Some(t), None) => Message::new_watermark(t.into()),
            (_, _) => unreachable!(),
        };
        Ok(Self { msg })
    }

    #[getter(data)]
    fn data<'a>(&self, py: Python<'a>) -> Option<&'a PyBytes> {
        match &self.msg {
            Message::TimestampedData(d) => Some(PyBytes::new(py, &d.data[..])),
            _ => None,
        }
    }

    #[getter(timestamp)]
    fn timestamp(&self) -> Option<PyTimestamp> {
        Some(self.msg.timestamp().clone().into())
    }

    fn is_timestamped_data(&self) -> bool {
        match &self.msg {
            Message::TimestampedData(_) => true,
            _ => false,
        }
    }

    fn is_watermark(&self) -> bool {
        match &self.msg {
            Message::Watermark(_) => true,
            _ => false,
        }
    }

    fn is_top_watermark(&self) -> bool {
        self.msg.is_top_watermark()
    }
}

impl From<Message<Vec<u8>>> for PyMessage {
    fn from(msg: Message<Vec<u8>>) -> Self {
        Self { msg }
    }
}

impl From<&PyMessage> for Message<Vec<u8>> {
    fn from(py_message: &PyMessage) -> Self {
        py_message.msg.clone()
    }
}

impl From<Arc<Message<Vec<u8>>>> for PyMessage {
    fn from(msg: Arc<Message<Vec<u8>>>) -> Self {
        Self {
            msg: (*msg).clone(),
        }
    }
}
