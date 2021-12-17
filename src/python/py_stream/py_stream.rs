use pyo3::prelude::*;
use pyo3::types::*;

use crate::dataflow::stream::Filter;
use crate::dataflow::stream::Map;
use crate::dataflow::stream::Split;
use crate::dataflow::stream::Stream;
use std::sync::Arc;

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
        let my_fn = Arc::new(map_fn);
        let f = move |data: &Vec<u8>| -> Vec<u8> {
            Python::with_gil(|py| {
                let serialized_data = PyBytes::new(py, &data[..]);
                my_fn
                    .call1(py, (serialized_data,))
                    .unwrap()
                    .extract(py)
                    .unwrap()
            })
        };

        Self {
            stream: self.stream.map(f),
        }
    }

    fn filter(&self, filter_fn: &PyAny) -> PyStream {
        Self {
            stream: self.stream.clone(),
        }
    }

    fn split(&self, split_fn: &PyAny) -> (PyStream, PyStream) {
        (
            Self {
                stream: self.stream.clone(),
            },
            Self {
                stream: self.stream.clone(),
            },
        )
    }

    // fn filter<F: 'static + Fn(Vec<u8>) -> bool + Send + Sync + Clone>(
    //     &self,
    //     filter_fn: F,
    // ) -> PyStream {
    //     Self {
    //         stream: self.stream.filter(filter_fn),
    //     }
    // }

    // fn split<F: 'static + Fn(Vec<u8>) -> bool + Send + Sync + Clone>(
    //     &self,
    //     split_fn: F,
    // ) -> (PyStream, Vec<u8>) {
    //     let (left_stream, right_stream) = self.stream.split(split_fn);
    //     (
    //         Self {
    //             stream: left_stream,
    //         },
    //         Self {
    //             stream: right_stream,
    //         },
    //     )
    // }
}

impl From<Stream<Vec<u8>>> for PyStream {
    fn from(stream: Stream<Vec<u8>>) -> Self {
        Self { stream }
    }
}
