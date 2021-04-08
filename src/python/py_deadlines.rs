use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::prelude::*;

use super::{PyReadStream, PyWriteStream};
use crate::dataflow::{stream::WriteStreamT, Message};
use crate::deadlines::*;

#[pyclass]
pub(crate) struct PyReceivingFrequencyDeadline {
    pub(crate) deadline: Option<ReceivingFrequencyDeadline>,
}

#[pymethods]
impl PyReceivingFrequencyDeadline {
    #[new]
    fn new(obj: &PyRawObject, py_read_stream: &PyReadStream, deadline_ms: u64, msg: String) {
        obj.init(Self {
            deadline: Some(
                ReceivingFrequencyDeadline::new(Duration::from_millis(deadline_ms))
                    .on_read_stream(&py_read_stream.read_stream)
                    .with_handler(move |t| {
                        slog::warn!(
                            crate::TERMINAL_LOGGER,
                            "Missed receiving frequency deadline @ {:?}: {}",
                            t,
                            msg
                        )
                    }),
            ),
        })
    }
}

#[pyclass]
pub(crate) struct PyTimestampDeadline {
    pub(crate) deadline: Option<TimestampDeadline>,
}

#[pymethods]
impl PyTimestampDeadline {
    #[new]
    fn new(
        obj: &PyRawObject,
        py_read_stream: &PyReadStream,
        py_write_stream: &PyWriteStream,
        deadline_ms: u64,
    ) {
        let write_stream_clone = Arc::new(Mutex::new(py_write_stream.write_stream.clone()));
        obj.init(Self {
            deadline: Some(
                TimestampDeadline::new(Duration::from_millis(deadline_ms))
                    .on_read_stream(&py_read_stream.read_stream)
                    .on_write_stream(&py_write_stream.write_stream)
                    .with_handler(move |t| {
                        slog::warn!(
                            crate::TERMINAL_LOGGER,
                            "Missed timestamp deadline @ {:?}. Sending watermark...",
                            t
                        );
                        write_stream_clone
                            .lock()
                            .unwrap()
                            .send(Message::new_watermark(t))
                            .unwrap();
                    }),
            ),
        })
    }
}
