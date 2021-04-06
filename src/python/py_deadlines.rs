use std::time::Duration;

use pyo3::prelude::*;

use super::PyReadStream;
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
                    .with_handler(move || {
                        slog::warn!(
                            crate::TERMINAL_LOGGER,
                            "Missed receiving frequency deadline: {}",
                            msg
                        )
                    }),
            ),
        })
    }
}
