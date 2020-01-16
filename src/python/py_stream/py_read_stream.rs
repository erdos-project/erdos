use pyo3::{exceptions, prelude::*, types::PyBytes};

use crate::dataflow::{stream::StreamId, Message, ReadStream};

use super::PyWriteStream;

fn process_message<'p>(
    py: Python<'p>,
    stream_id: StreamId,
    msg: Option<Message<Vec<u8>>>,
) -> PyResult<(Vec<u64>, Option<&'p PyBytes>)> {
    match msg {
        Some(Message::TimestampedData(msg)) => {
            Ok((msg.timestamp.time, Some(PyBytes::new(py, &msg.data[..]))))
        }
        Some(Message::Watermark(timestamp)) => Ok((timestamp.time, None)),
        None => Err(PyErr::new::<exceptions::Exception, _>(format!(
            "Unable to to read from stream: stream {} has no endpoints",
            stream_id
        ))),
    }
}

#[pyclass]
pub struct PyReadStream {
    pub read_stream: ReadStream<Vec<u8>>,
}

#[pymethods]
impl PyReadStream {
    #[new]
    fn new(obj: &PyRawObject) {
        obj.init(Self {
            read_stream: ReadStream::new(),
        });
    }

    /// Returns (timestamp, data)
    fn read<'p>(&mut self, py: Python<'p>) -> PyResult<(Vec<u64>, Option<&'p PyBytes>)> {
        process_message(py, self.read_stream.get_id(), self.read_stream.read())
    }

    fn try_read<'p>(
        &mut self,
        py: Python<'p>,
    ) -> PyResult<Option<(Vec<u64>, Option<&'p PyBytes>)>> {
        match self.read_stream.try_read() {
            Some(msg) => {
                Some(process_message(py, self.read_stream.get_id(), Some(msg))).transpose()
            }
            None => Ok(None),
        }
    }

    pub fn add_callback(&self, _py: Python, callback: PyObject) {
        self.read_stream.add_callback(move |_timestamp, data| {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let py_bytes = PyBytes::new(py, &data[..]);
            match callback.call1(py, (py_bytes,)) {
                Ok(_) => (),
                Err(e) => e.print(py),
            };
        })
    }

    pub fn add_watermark_callback(&self, _py: Python, callback: PyObject) {
        self.read_stream.add_watermark_callback(move |timestamp| {
            let gil = Python::acquire_gil();
            let py = gil.python();
            match callback.call1(py, (timestamp.time.clone(),)) {
                Ok(_) => (),
                Err(e) => e.print(py),
            };
        });
    }
}

impl From<&PyWriteStream> for PyReadStream {
    fn from(py_write_stream: &PyWriteStream) -> Self {
        Self {
            read_stream: ReadStream::from(&py_write_stream.write_stream),
        }
    }
}

impl From<ReadStream<Vec<u8>>> for PyReadStream {
    fn from(read_stream: ReadStream<Vec<u8>>) -> Self {
        Self { read_stream }
    }
}
