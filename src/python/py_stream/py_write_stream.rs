use pyo3::{exceptions, prelude::*};

use crate::{
    dataflow::{stream::WriteStreamT, Message, WriteStream},
    python::PyMessage,
};

#[pyclass]
pub struct PyWriteStream {
    pub write_stream: WriteStream<Vec<u8>>,
}

#[pymethods]
impl PyWriteStream {
    #[new]
    fn new(obj: &PyRawObject) {
        obj.init(Self {
            write_stream: WriteStream::new(),
        });
    }

    fn is_closed(&self) -> bool {
        self.write_stream.is_closed()
    }

    fn send(&mut self, msg: &PyMessage) -> PyResult<()> {
        self.write_stream.send(Message::from(msg)).map_err(|e| {
            exceptions::Exception::py_err(format!(
                "Error sending message on ingest stream {}: {:?}",
                self.write_stream.get_id(),
                e
            ))
        })
    }
}

impl From<WriteStream<Vec<u8>>> for PyWriteStream {
    fn from(write_stream: WriteStream<Vec<u8>>) -> Self {
        Self { write_stream }
    }
}
