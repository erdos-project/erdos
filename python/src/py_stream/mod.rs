use std::any::Any;

use erdos::dataflow::{
    operators::{Concat, Filter, Join, Map, Split},
    stream::Stream,
};
use pyo3::{prelude::*, types::PyBytes};

// Private submodules
mod py_egress_stream;
mod py_ingress_stream;
mod py_loop_stream;
mod py_operator_stream;
mod py_read_stream;
mod py_write_stream;

// Public exports
pub use py_egress_stream::PyEgressStream;
pub use py_ingress_stream::PyIngressStream;
pub use py_loop_stream::PyLoopStream;
pub use py_operator_stream::PyOperatorStream;
pub use py_read_stream::PyReadStream;
pub use py_write_stream::PyWriteStream;

/// The internal Python abstraction over a [`Stream`].
#[pyclass(subclass)]
pub struct PyStream {
    pub stream: Box<dyn Stream<Vec<u8>>>,
}

impl PyStream {
    pub fn downcast_stream<T: Any>(&mut self) -> Option<&mut T> {
        (&mut self.stream as &mut dyn Any).downcast_mut()
    }
}

#[pymethods]
impl PyStream {
    fn name(&self) -> String {
        self.stream.name()
    }

    fn id(&self) -> String {
        format!("{}", self.stream.id())
    }

    fn _map(&self, py: Python<'_>, function: PyObject) -> PyResult<Py<PyOperatorStream>> {
        let map_fn = move |data: &Vec<u8>| -> Vec<u8> {
            Python::with_gil(|py| {
                let serialized_data = PyBytes::new(py, &data[..]);
                function
                    .call1(py, (serialized_data,))
                    .unwrap()
                    .extract(py)
                    .unwrap()
            })
        };

        PyOperatorStream::new(py, self.stream.map(map_fn))
    }

    fn _flat_map(&self, py: Python<'_>, function: PyObject) -> PyResult<Py<PyOperatorStream>> {
        let flat_map_fn = move |data: &Vec<u8>| -> Vec<Vec<u8>> {
            Python::with_gil(|py| {
                let serialized_data = PyBytes::new(py, &data[..]);
                function
                    .call1(py, (serialized_data,))
                    .unwrap()
                    .extract(py)
                    .unwrap()
            })
        };
        PyOperatorStream::new(py, self.stream.flat_map(flat_map_fn))
    }

    fn _filter(&self, py: Python<'_>, function: PyObject) -> PyResult<Py<PyOperatorStream>> {
        let filter_fn = move |data: &Vec<u8>| -> bool {
            Python::with_gil(|py| {
                let serialized_data = PyBytes::new(py, &data[..]);
                function
                    .call1(py, (serialized_data,))
                    .unwrap()
                    .extract(py)
                    .unwrap()
            })
        };
        PyOperatorStream::new(py, self.stream.filter(filter_fn))
    }

    fn _split(
        &self,
        py: Python<'_>,
        function: PyObject,
    ) -> PyResult<(Py<PyOperatorStream>, Py<PyOperatorStream>)> {
        let split_fn = move |data: &Vec<u8>| -> bool {
            Python::with_gil(|py| {
                let serialized_data = PyBytes::new(py, &data[..]);
                function
                    .call1(py, (serialized_data,))
                    .unwrap()
                    .extract(py)
                    .unwrap()
            })
        };
        let (left_stream, right_stream) = self.stream.split(split_fn);
        Ok((
            PyOperatorStream::new(py, left_stream).unwrap(),
            PyOperatorStream::new(py, right_stream).unwrap(),
        ))
    }

    fn _timestamp_join(
        &self,
        py: Python<'_>,
        other: &PyStream,
        join_function: PyObject,
    ) -> PyResult<Py<PyOperatorStream>> {
        let map_fn = move |data: &(Vec<u8>, Vec<u8>)| -> Vec<u8> {
            Python::with_gil(|py| {
                let serialized_data_left = PyBytes::new(py, &data.0[..]);
                let serialized_data_right = PyBytes::new(py, &data.1[..]);
                join_function
                    .call1(py, (serialized_data_left, serialized_data_right))
                    .unwrap()
                    .extract(py)
                    .unwrap()
            })
        };
        PyOperatorStream::new(py, self.stream.timestamp_join(&other.stream).map(map_fn))
    }

    fn _concat(&self, py: Python<'_>, other: &PyStream) -> PyResult<Py<PyOperatorStream>> {
        PyOperatorStream::new(py, self.stream.concat(&other.stream))
    }
}
