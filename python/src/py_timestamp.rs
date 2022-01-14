use erdos::dataflow::Timestamp;
use pyo3::{basic::CompareOp, exceptions, prelude::*, PyObjectProtocol};

/// A Python version of ERDOS' Timestamp.
///
/// This struct breaks down the Timestamp enum into coordinates and booleans to represent Top and
/// Bottom.
#[pyclass]
#[derive(Clone)]
pub(crate) struct PyTimestamp {
    timestamp: Timestamp,
}

#[pymethods]
impl PyTimestamp {
    #[new]
    fn new(coordinates: Option<Vec<u64>>, is_top: bool, is_bottom: bool) -> PyResult<Self> {
        match (coordinates, is_top, is_bottom) {
            (None, true, false) => Ok(Self {
                timestamp: Timestamp::Top,
            }),
            (None, false, true) => Ok(Self {
                timestamp: Timestamp::Bottom,
            }),
            (Some(c), false, false) => Ok(Self {
                timestamp: Timestamp::Time(c),
            }),
            (_, _, _) => Err(exceptions::PyValueError::new_err(
                "Timestamp should either have coordinates or be either Top or Bottom.",
            )),
        }
    }

    fn is_top(&self) -> bool {
        self.timestamp.is_top()
    }

    fn is_bottom(&self) -> bool {
        self.timestamp.is_bottom()
    }

    fn coordinates(&self) -> Option<Vec<u64>> {
        match &self.timestamp {
            Timestamp::Time(c) => Some(c.clone()),
            _ => None,
        }
    }
}

#[pyproto]
impl PyObjectProtocol for PyTimestamp {
    fn __str__(&self) -> PyResult<String> {
        match &self.timestamp {
            Timestamp::Top => Ok(String::from("Timestamp::Top")),
            Timestamp::Bottom => Ok(String::from("Timestamp::Bottom")),
            Timestamp::Time(c) => Ok(format!("Timestamp::Time({:?})", c)),
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        self.__str__()
    }

    fn __richcmp__(&self, other: PyTimestamp, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Ok(self.timestamp < other.timestamp),
            CompareOp::Le => Ok(self.timestamp <= other.timestamp),
            CompareOp::Eq => Ok(self.timestamp == other.timestamp),
            CompareOp::Ne => Ok(self.timestamp != other.timestamp),
            CompareOp::Gt => Ok(self.timestamp > other.timestamp),
            CompareOp::Ge => Ok(self.timestamp >= other.timestamp),
        }
    }
}

impl From<Timestamp> for PyTimestamp {
    fn from(timestamp: Timestamp) -> Self {
        Self { timestamp }
    }
}

impl From<PyTimestamp> for Timestamp {
    fn from(timestamp: PyTimestamp) -> Self {
        timestamp.timestamp.clone()
    }
}
