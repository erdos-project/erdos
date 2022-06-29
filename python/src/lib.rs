#![feature(get_mut_unchecked)]

use erdos::node::NodeHandle;
use py_graph::PyGraph;
use pyo3::{exceptions, prelude::*};

// Private submodules
mod py_message;
mod py_operators;
mod py_stream;
mod py_timestamp;

pub mod py_graph;

// Private imports
use py_message::PyMessage;
use py_stream::{
    PyEgressStream, PyIngressStream, PyLoopStream, PyOperatorStream, PyReadStream, PyStream,
    PyWriteStream,
};
use py_timestamp::PyTimestamp;

#[pymodule]
fn internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyStream>()?;
    m.add_class::<PyOperatorStream>()?;
    m.add_class::<PyLoopStream>()?;
    m.add_class::<PyReadStream>()?;
    m.add_class::<PyWriteStream>()?;
    m.add_class::<PyIngressStream>()?;
    m.add_class::<PyEgressStream>()?;
    m.add_class::<PyMessage>()?;
    m.add_class::<PyTimestamp>()?;
    m.add_class::<PyGraph>()?;

    Ok(())
}

#[pyclass]
struct PyNodeHandle {
    node_handle: Option<NodeHandle>,
}

#[pymethods]
impl PyNodeHandle {
    fn shutdown_node(&mut self, py: Python) -> PyResult<()> {
        py.allow_threads(|| match self.node_handle.take() {
            Some(node_handle) => node_handle
                .shutdown()
                .map_err(exceptions::PyException::new_err),
            None => Err(exceptions::PyException::new_err(
                "Unable to shut down; no Rust node handle available",
            )),
        })
    }
}

impl From<NodeHandle> for PyNodeHandle {
    fn from(node_handle: NodeHandle) -> Self {
        Self {
            node_handle: Some(node_handle),
        }
    }
}
