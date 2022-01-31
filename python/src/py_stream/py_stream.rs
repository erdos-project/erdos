use std::sync::Arc;

use erdos::dataflow::{operators::Filter, operators::Map, operators::Split, stream::Stream};
use pyo3::prelude::*;
use pyo3::types::*;

/// The internal Python abstraction over an `Stream`.
///
/// This class is exposed on the Python interface as `erdos.streams.Stream`.
#[pyclass]
pub struct PyStream {
    pub stream: Stream<Vec<u8>>,
}

#[pymethods]
impl PyStream {
    fn name(&self) -> String {
        self.stream.name()
    }

    fn set_name(&mut self, name: String) {
        self.stream.set_name(&name)
    }

    fn id(&self) -> String {
        format!("{}", self.stream.id())
    }

    fn map(&self, map_fn: PyObject) -> PyStream {
        let new_fn = Arc::new(map_fn);
        let f = move |data: &Vec<u8>| -> Vec<u8> {
            Python::with_gil(|py| {
                let serialized_data = PyBytes::new(py, &data[..]);
                let pickle = PyModule::import(py, "pickle").unwrap();
                let des_data: PyObject = pickle
                    .getattr("loads")
                    .unwrap()
                    .call1((serialized_data,))
                    .unwrap()
                    .extract()
                    .unwrap();
                let result: PyObject = new_fn.call1(py, (des_data,)).unwrap().extract(py).unwrap();
                pickle
                    .getattr("dumps")
                    .unwrap()
                    .call1((result,))
                    .unwrap()
                    .extract()
                    .unwrap()
            })
        };

        Self {
            stream: self.stream.map(f),
        }
    }

    fn filter(&self, filter_fn: PyObject) -> PyStream {
        let new_fn = Arc::new(filter_fn);
        let f = move |data: &Vec<u8>| -> bool {
            Python::with_gil(|py| {
                let serialized_data = PyBytes::new(py, &data[..]);
                let pickle = PyModule::import(py, "pickle").unwrap();
                let des_data: PyObject = pickle
                    .getattr("loads")
                    .unwrap()
                    .call1((serialized_data,))
                    .unwrap()
                    .extract()
                    .unwrap();
                new_fn.call1(py, (des_data,)).unwrap().extract(py).unwrap()
            })
        };

        Self {
            stream: self.stream.filter(f),
        }
    }

    fn split(&self, split_fn: PyObject) -> (PyStream, PyStream) {
        let new_fn = Arc::new(split_fn);
        let f = move |data: &Vec<u8>| -> bool {
            Python::with_gil(|py| {
                let serialized_data = PyBytes::new(py, &data[..]);
                let pickle = PyModule::import(py, "pickle").unwrap();
                let des_data: PyObject = pickle
                    .getattr("loads")
                    .unwrap()
                    .call1((serialized_data,))
                    .unwrap()
                    .extract()
                    .unwrap();
                new_fn.call1(py, (des_data,)).unwrap().extract(py).unwrap()
            })
        };

        let (left_stream, right_stream) = self.stream.split(f);

        (
            Self {
                stream: left_stream,
            },
            Self {
                stream: right_stream,
            },
        )
    }
}

impl From<Stream<Vec<u8>>> for PyStream {
    fn from(stream: Stream<Vec<u8>>) -> Self {
        Self { stream }
    }
}
