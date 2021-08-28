use std::sync::{Arc, Mutex};

use pyo3::{exceptions, prelude::*};
use slog;

use crate::{
    dataflow::{graph::default_graph, OperatorConfig, Stream, StreamT},
    node::{operator_executors::OperatorExecutorT, Node, NodeHandle, NodeId},
    scheduler::channel_manager::ChannelManager,
    Configuration, OperatorId,
};

// Private submodules
mod py_executors;
mod py_message;
mod py_operators;
mod py_stream;
mod py_timestamp;

// Private imports
use py_executors::PySourceExecutor;
use py_message::PyMessage;
use py_operators::{PyOneInOneOut, PyOneInTwoOut, PySink};
use py_stream::{
    PyExtractStream, PyIngestStream, PyLoopStream, PyReadStream, PyStream, PyWriteStream,
};
use py_timestamp::PyTimestamp;

#[pymodule]
fn internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyStream>()?;
    m.add_class::<PyLoopStream>()?;
    m.add_class::<PyReadStream>()?;
    m.add_class::<PyWriteStream>()?;
    m.add_class::<PyIngestStream>()?;
    m.add_class::<PyExtractStream>()?;
    m.add_class::<PyMessage>()?;
    m.add_class::<PyTimestamp>()?;

    #[pyfn(m)]
    #[pyo3(name = "connect_source")]
    fn connect_source_py(
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        args: PyObject,
        kwargs: PyObject,
        node_id: NodeId,
    ) -> PyResult<PyStream> {
        // Create the WriteStream.
        let write_stream = Stream::new();
        let write_stream_ids = vec![write_stream.id()];
        let write_stream_ids_clone = write_stream_ids.clone();

        // Create the config.
        let operator_name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name = match &operator_name {
            Some(n) => n.clone(),
            None => String::from("SourceOperator"),
        };
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let mut config = OperatorConfig::new()
            .name(&name)
            .node(node_id)
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Assigning ID {} to {}.",
            config.id,
            name,
        );
        let config_copy = config.clone();

        // Arc objects to pass to the executor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        // Create the operator runner.
        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let write_stream = channel_manager
                    .get_write_stream(write_stream_ids_clone[0])
                    .unwrap();

                Box::new(PySourceExecutor::new(
                    Arc::clone(&py_type_arc),
                    Arc::clone(&args_arc),
                    Arc::clone(&kwargs_arc),
                    Arc::clone(&py_config_arc),
                    config_copy.clone(),
                    write_stream,
                ))
            };

        // Add the operator and WriteStream to the graph.
        default_graph::add_operator(
            config.id,
            config.name,
            config.node_id,
            vec![],
            write_stream_ids,
            op_runner,
        );
        default_graph::add_operator_stream(config.id, &write_stream);

        Ok(PyStream::from(write_stream))
    }

    #[pyfn(m)]
    #[pyo3(name = "connect_sink")]
    fn connect_sink_py(
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        read_stream: &PyStream,
        args: PyObject,
        kwargs: PyObject,
        node_id: NodeId,
    ) -> PyResult<()> {
        // Create the config.
        let operator_name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name = match &operator_name {
            Some(n) => n.clone(),
            None => String::from("SinkOperator"),
        };
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let mut config = OperatorConfig::new()
            .name(&name)
            .node(node_id)
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Assigning ID {} to {}.",
            config.id,
            name,
        );
        let config_copy = config.clone();

        // Arc objects to pass to the constructor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        crate::connect_sink(
            move || -> PySink {
                PySink::new(
                    Arc::clone(&py_type_arc),
                    Arc::clone(&args_arc),
                    Arc::clone(&kwargs_arc),
                    Arc::clone(&py_config_arc),
                    config_copy.clone(),
                )
            },
            || {},
            config,
            &read_stream.stream,
        );
        Ok(())
    }

    #[pyfn(m)]
    #[pyo3(name = "connect_one_in_one_out")]
    fn connect_one_in_one_out_py(
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        read_stream: &PyStream,
        args: PyObject,
        kwargs: PyObject,
        node_id: NodeId,
    ) -> PyResult<PyStream> {
        // Create the config.
        let operator_name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name = match &operator_name {
            Some(n) => n.clone(),
            None => String::from("OneInOneOut"),
        };
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let mut config = OperatorConfig::new()
            .name(&name)
            .node(node_id)
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Assigning ID {} to {}.",
            config.id,
            name,
        );
        let config_copy = config.clone();

        // Arc objects to pass to the executor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        let write_stream = crate::connect_one_in_one_out(
            move || -> PyOneInOneOut {
                PyOneInOneOut::new(
                    Arc::clone(&py_type_arc),
                    Arc::clone(&args_arc),
                    Arc::clone(&kwargs_arc),
                    Arc::clone(&py_config_arc),
                    config_copy.clone(),
                )
            },
            || {},
            config,
            &read_stream.stream,
        );

        Ok(PyStream::from(write_stream))
    }

    #[pyfn(m)]
    #[pyo3(name = "connect_one_in_two_out")]
    fn connect_one_in_two_out_py(
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        read_stream: &PyStream,
        args: PyObject,
        kwargs: PyObject,
        node_id: NodeId,
    ) -> PyResult<(PyStream, PyStream)> {
        // Create the config.
        let operator_name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name = match &operator_name {
            Some(n) => n.clone(),
            None => String::from("OneInTwoOut"),
        };
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let mut config = OperatorConfig::new()
            .name(&name)
            .node(node_id)
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Assigning ID {} to {}",
            config.id,
            name,
        );
        let config_copy = config.clone();

        // Arc objects to pass to the executor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        let (left_write_stream, right_write_stream) = crate::connect_one_in_two_out(
            move || -> PyOneInTwoOut {
                PyOneInTwoOut::new(
                    Arc::clone(&py_type_arc),
                    Arc::clone(&args_arc),
                    Arc::clone(&kwargs_arc),
                    Arc::clone(&py_config_arc),
                    config_copy.clone(),
                )
            },
            || {},
            config,
            &read_stream.stream,
        );

        Ok((
            PyStream::from(left_write_stream),
            PyStream::from(right_write_stream),
        ))
    }

    #[pyfn(m)]
    #[pyo3(name = "reset")]
    fn reset_py() {
        crate::reset();
    }

    #[pyfn(m)]
    #[pyo3(name = "run")]
    fn run_py(
        py: Python,
        node_id: NodeId,
        data_addresses: Vec<String>,
        control_addresses: Vec<String>,
        graph_filename: Option<String>,
    ) -> PyResult<()> {
        py.allow_threads(move || {
            let data_addresses = data_addresses
                .into_iter()
                .map(|s| s.parse().expect("Unable to parse socket address"))
                .collect();
            let control_addresses = control_addresses
                .into_iter()
                .map(|s| s.parse().expect("Unable to parse socket address"))
                .collect();
            let config = Configuration::new(
                node_id,
                data_addresses,
                control_addresses,
                7,
                graph_filename,
            );
            let mut node = Node::new(config);
            node.run();
        });
        Ok(())
    }

    #[pyfn(m)]
    #[pyo3(name = "run_async")]
    fn run_async_py(
        py: Python,
        node_id: NodeId,
        data_addresses: Vec<String>,
        control_addresses: Vec<String>,
        graph_filename: Option<String>,
    ) -> PyResult<PyNodeHandle> {
        let node_handle = py.allow_threads(move || {
            let data_addresses = data_addresses
                .into_iter()
                .map(|s| s.parse().expect("Unable to parse socket address"))
                .collect();
            let control_addresses = control_addresses
                .into_iter()
                .map(|s| s.parse().expect("Unable to parse socket address"))
                .collect();
            let config = Configuration::new(
                node_id,
                data_addresses,
                control_addresses,
                7,
                graph_filename,
            );
            let node = Node::new(config);
            node.run_async()
        });
        Ok(PyNodeHandle::from(node_handle))
    }

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
