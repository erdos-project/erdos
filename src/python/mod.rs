use pyo3::{exceptions, prelude::*, types::*};

use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    communication::ControlMessage,
    dataflow::{
        graph::default_graph, stream::InternalReadStream, Operator, OperatorConfig, ReadStream,
        WriteStream,
    },
    node::{
        node::NodeHandle,
        operator_executor::{OperatorExecutor, OperatorExecutorStream, OperatorExecutorStreamT},
        Node, NodeId,
    },
    scheduler::channel_manager::ChannelManager,
    Configuration, Uuid,
};

mod py_message;
mod py_stream;

use py_stream::{PyExtractStream, PyIngestStream, PyLoopStream, PyReadStream, PyWriteStream};

pub(crate) use py_message::PyMessage;

#[pymodule]
fn internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyLoopStream>()?;
    m.add_class::<PyReadStream>()?;
    m.add_class::<PyWriteStream>()?;
    m.add_class::<PyIngestStream>()?;
    m.add_class::<PyExtractStream>()?;
    m.add_class::<PyMessage>()?;

    #[pyfn(m, "connect")]
    fn connect_py(
        py: Python,
        py_type: PyObject,
        py_config: PyObject,
        read_streams_obj: PyObject,
        args: PyObject,
        kwargs: PyObject,
        node_id: NodeId,
    ) -> PyResult<Vec<PyReadStream>> {
        // Call Operator.connect(*read_streams) to get write streams
        let locals = PyDict::new(py);
        locals.set_item("Operator", py_type.clone_ref(py))?;
        locals.set_item("config", py_config.clone_ref(py))?;
        locals.set_item("read_streams", read_streams_obj.clone_ref(py))?;
        let streams_result = py.eval(
            "[s._py_write_stream for s in Operator.connect(*read_streams)]",
            None,
            Some(&locals),
        )?;
        let connect_read_streams: Vec<&PyReadStream> = read_streams_obj.extract(py)?;
        let connect_write_streams: Vec<&PyWriteStream> = streams_result.extract()?;

        // Register the operator
        let op_id = crate::OperatorId::new_deterministic();
        let name: Option<String> = py_config.getattr(py, "name")?.extract(py)?;
        let name_clone = name.clone();
        let flow_watermarks: bool = py_config.getattr(py, "flow_watermarks")?.extract(py)?;
        let read_stream_ids: Vec<Uuid> = connect_read_streams
            .iter()
            .map(|rs| rs.read_stream.get_id())
            .collect();
        let read_stream_ids_clone = read_stream_ids.clone();
        let write_stream_ids: Vec<Uuid> = connect_write_streams
            .iter()
            .map(|ws| ws.write_stream.get_id())
            .collect();
        let write_stream_ids_clone = write_stream_ids.clone();

        // Arc objects to allow cloning the closure
        let py_type_arc = Arc::new(py_type);
        let py_config_arc = Arc::new(py_config);
        let args_arc = Arc::new(args);
        let kwargs_arc = Arc::new(kwargs);

        let operator_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>,
                  control_sender: UnboundedSender<ControlMessage>,
                  mut control_receiver: UnboundedReceiver<ControlMessage>| {
                // Create python streams from endpoints
                let py_read_streams: Vec<PyReadStream> = read_stream_ids_clone
                    .clone()
                    .iter()
                    .map(|&id| {
                        let recv_endpoint = channel_manager
                            .lock()
                            .unwrap()
                            .take_recv_endpoint(id)
                            .unwrap();
                        PyReadStream::from(ReadStream::from(InternalReadStream::from_endpoint(
                            recv_endpoint,
                            id,
                        )))
                    })
                    .collect();
                let py_write_streams: Vec<PyWriteStream> = write_stream_ids_clone
                    .iter()
                    .map(|&id| {
                        let send_endpoints = channel_manager
                            .lock()
                            .unwrap()
                            .get_send_endpoints(id)
                            .unwrap();
                        PyWriteStream::from(WriteStream::from_endpoints(send_endpoints, id))
                    })
                    .collect();

                // Create operator executor streams from read streams
                let mut op_ex_streams: Vec<Box<dyn OperatorExecutorStreamT>> = Vec::new();
                for py_read_stream in py_read_streams.iter() {
                    op_ex_streams.push(Box::new(OperatorExecutorStream::from(
                        &py_read_stream.read_stream,
                    )));
                }

                // Instantiate and run the operator in Python
                let gil = Python::acquire_gil();
                let py = gil.python();
                let locals = PyDict::new(py);
                let py_read_streams: Vec<PyRef<PyReadStream>> = py_read_streams
                    .into_iter()
                    .map(|rs| PyRef::new(py, rs).unwrap())
                    .collect();
                let py_write_streams: Vec<PyRef<PyWriteStream>> = py_write_streams
                    .into_iter()
                    .map(|ws| PyRef::new(py, ws).unwrap())
                    .collect();
                locals
                    .set_item("Operator", py_type_arc.clone_ref(py))
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("py_read_streams", py_read_streams)
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("py_write_streams", py_write_streams)
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("op_id", format!("{}", op_id))
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("config", py_config_arc.clone_ref(py))
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("flow_watermarks", flow_watermarks)
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("args", args_arc.clone_ref(py))
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("kwargs", kwargs_arc.clone_ref(py))
                    .err()
                    .map(|e| e.print(py));
                // NOTE: Do not use list comprehension in py.run because it causes a crashes the
                // Python processes. We do not currently know why this is the case.
                // Initialize operator
                let py_result = py.run(
                    r#"
import uuid

import erdos

read_streams = []
for i in range(len(py_read_streams)):
    read_streams.append(erdos.ReadStream(_py_read_stream=py_read_streams[i]))

write_streams = []
for i in range(len(py_write_streams)):
    write_streams.append(erdos.WriteStream(_py_write_stream=py_write_streams[i]))

operator = Operator.__new__(Operator)
operator._id = uuid.UUID(op_id)
operator._config = config
trace_logger_name = "{}-profile".format(type(operator) if config.name is None else config.name)
operator._trace_event_logger = erdos.utils.setup_trace_logging(trace_logger_name, config.profile_file_name)
operator.__init__(*read_streams, *write_streams, *args, **kwargs)

if flow_watermarks and len(read_streams) > 0 and len(write_streams) > 0:
   erdos.add_watermark_callback(read_streams, write_streams, erdos._flow_watermark_callback)
"#,
                    None,
                    Some(&locals),
                );
                if let Err(e) = py_result {
                    e.print(py)
                }
                // Notify node that operator is done setting up
                let logger = crate::get_terminal_logger();
                if let Err(e) = control_sender.send(ControlMessage::OperatorInitialized(op_id)) {
                    error!(
                        logger,
                        "Error sending OperatorInitialized message to control handler: {:?}", e
                    );
                }

                let operator_obj = py
                    .eval("operator", None, Some(&locals))
                    .unwrap()
                    .to_object(py);
                let operator_arc = Arc::new(operator_obj);

                let mut config: OperatorConfig<()> = OperatorConfig::new();
                config.name = name_clone.clone();
                config.id = op_id;
                config.flow_watermarks = flow_watermarks;
                config.node_id = node_id;
                OperatorExecutor::new(
                    PyOperator {
                        operator: operator_arc,
                    },
                    config,
                    op_ex_streams,
                    control_receiver,
                    logger,
                )
            };

        default_graph::add_operator(
            op_id,
            name,
            node_id,
            read_stream_ids,
            write_stream_ids,
            operator_runner,
        );

        let result = connect_write_streams
            .iter()
            .map(|&ws| PyReadStream::from(ws))
            .collect();

        for py_write_stream in connect_write_streams.iter() {
            default_graph::add_operator_stream(op_id, &py_write_stream.write_stream);
        }

        Ok(result)
    }

    #[pyfn(m, "reset")]
    fn reset_py() {
        crate::reset();
    }

    #[pyfn(m, "run")]
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

    #[pyfn(m, "run_async")]
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

    #[pyfn(m, "add_watermark_callback")]
    fn add_watermark_callback_py(
        read_streams: Vec<&PyReadStream>,
        callback: PyObject,
    ) -> PyResult<()> {
        py_stream::add_watermark_callback(read_streams, callback)
    }

    Ok(())
}

struct PyOperator {
    operator: Arc<PyObject>,
}

impl Operator for PyOperator {
    fn run(&mut self) {
        let gil = Python::acquire_gil();
        let py = gil.python();
        if let Err(e) = self.operator.call_method0(py, "run") {
            e.print(py);
        }
    }

    fn destroy(&mut self) {
        let gil = Python::acquire_gil();
        let py = gil.python();
        if let Err(e) = self.operator.call_method0(py, "destroy") {
            e.print(py);
        }
    }
}

#[pyclass]
pub(crate) struct PyNodeHandle {
    node_handle: Option<NodeHandle>,
}

#[pymethods]
impl PyNodeHandle {
    fn shutdown_node(&mut self, py: Python) -> PyResult<()> {
        py.allow_threads(|| match self.node_handle.take() {
            Some(node_handle) => node_handle
                .shutdown()
                .map_err(exceptions::Exception::py_err),
            None => Err(exceptions::Exception::py_err(
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
