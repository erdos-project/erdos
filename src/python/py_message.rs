use pyo3::{exceptions, prelude::*, types::PyBytes};

use crate::dataflow::{Message, Timestamp};

#[pyclass]
pub(crate) struct PyMessage {
    msg: Message<Vec<u8>>,
}

#[pymethods]
impl PyMessage {
    #[new]
    fn new<'a>(
        obj: &PyRawObject,
        timestamp_coordinates: Option<Vec<u64>>,
        is_top_watermark: bool,
        data: Option<&'a PyBytes>,
    ) -> PyResult<()> {
        if timestamp_coordinates.is_none() && data.is_some() {
            return Err(exceptions::ValueError::py_err(
                "Passing a non-None value to data when timestamp_coordinates=None is not allowed",
            ));
        }
        let msg = if is_top_watermark {
            Message::new_top_watermark()
        } else {
            match (timestamp_coordinates, data) {
                (Some(t), Some(d)) => {
                    Message::new_message(Timestamp::new(t), Vec::from(d.as_bytes()))
                }
                (Some(t), None) => Message::new_watermark(Timestamp::new(t)),
                (_, _) => unreachable!(),
            }
        };
        obj.init(Self { msg });
        Ok(())
    }

    #[getter(data)]
    fn data<'a>(&self, py: Python<'a>) -> Option<&'a PyBytes> {
        match &self.msg {
            Message::TimestampedData(d) => Some(PyBytes::new(py, &d.data[..])),
            _ => None,
        }
    }

    #[getter(timestamp)]
    fn timestamp(&self) -> Option<Vec<u64>> {
        match &self.msg {
            Message::TimestampedData(d) => Some(d.timestamp.time.clone()),
            Message::Watermark(t) => Some(t.time.clone()),
        }
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
