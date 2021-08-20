use crate::dataflow::Timestamp;
use pyo3::{exceptions, prelude::*};

/// A Python version of ERDOS' Timestamp.
///
/// This struct breaks down the Timestamp enum into coordinates and booleans to represent Top and
/// Bottom.
#[pyclass]
#[derive(Clone)]
pub(crate) struct PyTimestamp {
    #[pyo3(get)]
    coordinates: Option<Vec<u64>>,
    #[pyo3(get)]
    is_top: bool,
    #[pyo3(get)]
    is_bottom: bool,
}

#[pymethods]
impl PyTimestamp {
    #[new]
    fn new(coordinates: Option<Vec<u64>>, is_top: bool, is_bottom: bool) -> PyResult<Self> {
        match (coordinates, is_top, is_bottom) {
            (None, true, false) => Ok(Self {
                coordinates: None,
                is_top,
                is_bottom,
            }),
            (None, false, true) => Ok(Self {
                coordinates: None,
                is_top,
                is_bottom,
            }),
            (Some(c), false, false) => Ok(Self {
                coordinates: Some(c),
                is_top,
                is_bottom,
            }),
            (_, _, _) => Err(exceptions::PyValueError::new_err(
                "Timestamp should either have coordinates or be either Top or Bottom.",
            )),
        }
    }
}

impl From<Timestamp> for PyTimestamp {
    fn from(timestamp: Timestamp) -> Self {
        match timestamp {
            Timestamp::Bottom => Self {
                coordinates: None,
                is_top: false,
                is_bottom: true,
            },
            Timestamp::Time(c) => Self {
                coordinates: Some(c),
                is_top: false,
                is_bottom: false,
            },
            Timestamp::Top => Self {
                coordinates: None,
                is_top: true,
                is_bottom: false,
            },
        }
    }
}

impl Into<Timestamp> for PyTimestamp {
    fn into(self) -> Timestamp {
        if self.is_bottom {
            return Timestamp::Bottom;
        } else if self.is_top {
            return Timestamp::Top;
        } else {
            return Timestamp::Time(self.coordinates.unwrap());
        }
    }
}
