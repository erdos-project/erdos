use std::sync::Arc;

use pyo3::{pyclass, pymethods, Py, PyObject, PyResult, Python};

use erdos::{dataflow::Graph, node::WorkerId, Configuration, OperatorConfig, OperatorId};

use crate::{
    py_operators::{PyOneInOneOut, PyOneInTwoOut, PySink, PySource, PyTwoInOneOut},
    py_stream::{PyIngressStream, PyLoopStream, PyStream},
    PyOperatorStream,
};

#[pyclass]
pub(crate) struct PyGraph {
    graph: Graph,
}

#[pymethods]
impl PyGraph {
    #[new]
    fn new(name: &str) -> PyResult<Self> {
        Ok(Self {
            graph: Graph::new(name),
        })
    }

    fn add_ingress(&self, py: Python, name: &str) -> Py<PyIngressStream> {
        PyIngressStream::new(py, self.graph.add_ingress(name)).unwrap()
    }

    fn add_loop_stream(&self, py: Python) -> Py<PyLoopStream> {
        PyLoopStream::new(py, self.graph.add_loop_stream()).unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_source(
        &self,
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        args: PyObject,
        kwargs: PyObject,
        worker_id: usize,
    ) -> PyResult<Py<PyOperatorStream>> {
        // Create the config.
        let operator_name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name = match &operator_name {
            Some(n) => n.clone(),
            None => String::from("SourceOperator"),
        };
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let mut config = OperatorConfig::new()
            .name(&name)
            .worker(WorkerId(worker_id))
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        tracing::debug!("Assigning ID {} to {}.", config.id, name,);
        let config_copy = config.clone();

        // Arc objects to pass to the executor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        let write_stream = self.graph.connect_source(
            move || -> PySource {
                PySource::new(
                    Arc::clone(&py_type_arc),
                    Arc::clone(&args_arc),
                    Arc::clone(&kwargs_arc),
                    Arc::clone(&py_config_arc),
                    config_copy.clone(),
                )
            },
            config,
        );

        PyOperatorStream::new(py, write_stream)
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_sink(
        &self,
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        read_stream: &PyStream,
        args: PyObject,
        kwargs: PyObject,
        worker_id: usize,
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
            .worker(WorkerId(worker_id))
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        tracing::debug!("Assigning ID {} to {}.", config.id, name,);
        let config_copy = config.clone();

        // Arc objects to pass to the constructor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        self.graph.connect_sink(
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
            read_stream.stream.as_ref(),
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_one_in_one_out(
        &self,
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        read_stream: &PyStream,
        args: PyObject,
        kwargs: PyObject,
        worker_id: usize,
    ) -> PyResult<Py<PyOperatorStream>> {
        // Create the config.
        let operator_name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name = match &operator_name {
            Some(n) => n.clone(),
            None => String::from("OneInOneOut"),
        };
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let mut config = OperatorConfig::new()
            .name(&name)
            .worker(WorkerId(worker_id))
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        tracing::debug!("Assigning ID {} to {}.", config.id, name,);
        let config_copy = config.clone();

        // Arc objects to pass to the executor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        let write_stream = self.graph.connect_one_in_one_out(
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
            read_stream.stream.as_ref(),
        );

        PyOperatorStream::new(py, write_stream)
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_one_in_two_out(
        &self,
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        read_stream: &PyStream,
        args: PyObject,
        kwargs: PyObject,
        worker_id: usize,
    ) -> PyResult<(Py<PyOperatorStream>, Py<PyOperatorStream>)> {
        // Create the config.
        let operator_name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name = match &operator_name {
            Some(n) => n.clone(),
            None => String::from("OneInTwoOut"),
        };
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let mut config = OperatorConfig::new()
            .name(&name)
            .worker(WorkerId(worker_id))
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        tracing::debug!("Assigning ID {} to {}", config.id, name,);
        let config_copy = config.clone();

        // Arc objects to pass to the executor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        let (left_write_stream, right_write_stream) = self.graph.connect_one_in_two_out(
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
            read_stream.stream.as_ref(),
        );

        let py_left_write_stream = PyOperatorStream::new(py, left_write_stream)?;
        let py_right_write_stream = PyOperatorStream::new(py, right_write_stream)?;

        Ok((py_left_write_stream, py_right_write_stream))
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_two_in_one_out(
        &self,
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        left_read_stream: &PyStream,
        right_read_stream: &PyStream,
        args: PyObject,
        kwargs: PyObject,
        worker_id: usize,
    ) -> PyResult<Py<PyOperatorStream>> {
        // Create the config.
        let operator_name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name = match &operator_name {
            Some(n) => n.clone(),
            None => String::from("OneInOneOut"),
        };
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let mut config = OperatorConfig::new()
            .name(&name)
            .worker(WorkerId(worker_id))
            .flow_watermarks(flow_watermarks);
        config.id = OperatorId::new_deterministic();
        tracing::debug!("Assigning ID {} to {}.", config.id, name,);
        let config_copy = config.clone();

        // Arc objects to pass to the executor.
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        let write_stream = self.graph.connect_two_in_one_out(
            move || -> PyTwoInOneOut {
                PyTwoInOneOut::new(
                    Arc::clone(&py_type_arc),
                    Arc::clone(&args_arc),
                    Arc::clone(&kwargs_arc),
                    Arc::clone(&py_config_arc),
                    config_copy.clone(),
                )
            },
            || {},
            config,
            left_read_stream.stream.as_ref(),
            right_read_stream.stream.as_ref(),
        );

        PyOperatorStream::new(py, write_stream)
    }

    // TODO: fix for leader-worker.
    // fn run(
    //     &self,
    //     py: Python,
    //     worker_id: usize,
    //     data_addresses: Vec<String>,
    //     control_addresses: Vec<String>,
    //     graph_filename: Option<String>,
    // ) -> PyResult<()> {
    //     py.allow_threads(move || {
    //         let data_addresses = data_addresses
    //             .into_iter()
    //             .map(|s| s.parse().expect("Unable to parse socket address"))
    //             .collect();
    //         let control_addresses = control_addresses
    //             .into_iter()
    //             .map(|s| s.parse().expect("Unable to parse socket address"))
    //             .collect();
    //         let mut config = Configuration::new(node_id, data_addresses, control_addresses, 7);
    //         if let Some(filename) = graph_filename {
    //             config = config.export_dataflow_graph(filename.as_str());
    //         }

    //         let mut node = Node::new(config);
    //         node.run(self.graph.clone());
    //     });
    //     Ok(())
    // }

    // fn run_async(
    //     &self,
    //     py: Python,
    //     worker_id: usize,
    //     data_addresses: Vec<String>,
    //     control_addresses: Vec<String>,
    //     graph_filename: Option<String>,
    // ) -> PyResult<PyNodeHandle> {
    //     let node_handle = py.allow_threads(move || {
    //         let data_addresses = data_addresses
    //             .into_iter()
    //             .map(|s| s.parse().expect("Unable to parse socket address"))
    //             .collect();
    //         let control_addresses = control_addresses
    //             .into_iter()
    //             .map(|s| s.parse().expect("Unable to parse socket address"))
    //             .collect();
    //         let mut config = Configuration::new(node_id, data_addresses, control_addresses, 7);
    //         if let Some(filename) = graph_filename {
    //             config = config.export_dataflow_graph(filename.as_str());
    //         }

    //         let node = Node::new(config);
    //         node.run_async(self.graph.clone())
    //     });
    //     Ok(PyNodeHandle::from(node_handle))
    // }
}
