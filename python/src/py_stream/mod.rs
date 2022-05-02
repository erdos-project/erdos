use erdos::dataflow::{
    operators::{Concat, Filter, Join, Map, Split},
    stream::{Stream, StreamId},
};
use pyo3::{prelude::*, types::PyBytes};

// Private submodules
mod py_extract_stream;
mod py_ingest_stream;
mod py_loop_stream;
mod py_operator_stream;
mod py_read_stream;
mod py_write_stream;

// Public exports
pub use py_extract_stream::PyExtractStream;
pub use py_ingest_stream::PyIngestStream;
pub use py_loop_stream::PyLoopStream;
pub use py_operator_stream::PyOperatorStream;
pub use py_read_stream::PyReadStream;
pub use py_write_stream::PyWriteStream;

/// The internal Python abstraction over a [`Stream`].
#[pyclass(subclass)]
pub struct PyStream {
    pub id: StreamId,
}

#[pymethods]
impl PyStream {
    fn name(&self) -> String {
        Stream::name(self)
    }

    fn set_name(&mut self, name: &str) {
        Stream::set_name(self, name)
    }

    fn id(&self) -> String {
        format!("{}", self.id)
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
        PyOperatorStream::new(py, self.map(map_fn))
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
        PyOperatorStream::new(py, self.flat_map(flat_map_fn))
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
        PyOperatorStream::new(py, self.filter(filter_fn))
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
        let (left_stream, right_stream) = self.split(split_fn);
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
        PyOperatorStream::new(py, self.timestamp_join(other).map(map_fn))
    }

    fn _concat(&self, py: Python<'_>, other: &PyStream) -> PyResult<Py<PyOperatorStream>> {
        PyOperatorStream::new(py, self.concat(other))
    }
}

impl Stream<Vec<u8>> for PyStream {
    fn id(&self) -> StreamId {
        self.id
    }
}
