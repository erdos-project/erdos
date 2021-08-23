use std::{collections::HashSet, sync::Arc};

use crate::{
    dataflow::{
        operator::OperatorConfig, stream::WriteStreamT, Message, StreamT, Timestamp, WriteStream,
    },
    node::operator_event::{OperatorEvent, OperatorType},
    python::{py_executors::PyOneInMessageProcessorT, py_stream::PyWriteStream, PyTimestamp},
};

use pyo3::{prelude::*, types::*};

pub struct PyOneInOneOutMessageProcessor {
    py_operator_config: Arc<PyObject>,
    py_operator: Arc<PyObject>,
    py_write_stream: Arc<PyObject>,
    write_stream: WriteStream<Vec<u8>>,
    config: OperatorConfig,
}

impl PyOneInOneOutMessageProcessor {
    pub(crate) fn new(
        py_operator_type: Arc<PyObject>,
        py_operator_args: Arc<PyObject>,
        py_operator_kwargs: Arc<PyObject>,
        py_operator_config: Arc<PyObject>,
        config: OperatorConfig,
        write_stream: WriteStream<Vec<u8>>,
    ) -> Self {
        // Construct the Python version of the WriteStream.
        let write_stream_clone = write_stream.clone();
        let write_stream_id = write_stream.id().clone();
        let py_write_stream = PyWriteStream::from(write_stream);

        // Instantiate the operator in Python.
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Instantiating the operator {:?}",
            config.name
        );

        // Create the locals to run the constructor.
        let (py_operator, py_write_stream_obj) = Python::with_gil(|py| -> (PyObject, PyObject) {
            let locals = PyDict::new(py);
            locals
                .set_item("Operator", py_operator_type.clone_ref(py))
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("op_id", format!("{}", config.id))
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("args", py_operator_args.clone_ref(py))
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("kwargs", py_operator_kwargs.clone_ref(py))
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("config", py_operator_config.clone_ref(py))
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("op_name", format!("{}", config.get_name()))
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("py_write_stream", &Py::new(py, py_write_stream).unwrap())
                .err()
                .map(|e| e.print(py));
            locals
                .set_item(
                    "write_stream_name",
                    format!("{}_WriteStream", config.get_name()),
                )
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("write_stream_id", format!("{}", write_stream_id))
                .err()
                .map(|e| e.print(py));

            // Initialize the operator.
            let init_result = py.run(
                r#"
import uuid, erdos

# Create the WriteStream.
write_stream = erdos.WriteStream(
    _py_write_stream=py_write_stream,
    _name=write_stream_name,
    _id=uuid.UUID(write_stream_id),
)

# Create the operator.
operator = Operator.__new__(Operator)
operator._id = uuid.UUID(op_id)
operator._config = config
operator._trace_event_logger = erdos.utils.setup_trace_logging(
    "{}-profile".format(op_name), 
    config.profile_file_name,
)
operator.__init__(*args, **kwargs)
            "#,
                None,
                Some(&locals),
            );
            if let Err(e) = init_result {
                e.print(py);
            }

            // Retrieve the constructed operator.
            let py_operator = py
                .eval("operator", None, Some(&locals))
                .unwrap()
                .to_object(py);

            // Retrive the constructed WriteStream.
            let py_write_stream_obj = py
                .eval("write_stream", None, Some(&locals))
                .unwrap()
                .to_object(py);

            (py_operator, py_write_stream_obj)
        });
        Self {
            py_operator_config,
            py_operator: Arc::new(py_operator),
            py_write_stream: Arc::new(py_write_stream_obj),
            write_stream: write_stream_clone,
            config,
        }
    }
}

impl PyOneInMessageProcessorT for PyOneInOneOutMessageProcessor {
    fn execute_run(&mut self, read_stream: &PyObject) {
        let py_write_stream = Arc::clone(&self.py_write_stream);
        Python::with_gil(|py| {
            if let Err(e) = self.py_operator.call_method1(
                py,
                "run",
                (read_stream, py_write_stream.clone_ref(py)),
            ) {
                e.print(py);
            };
        });
    }

    fn execute_destroy(&mut self) {
        Python::with_gil(|py| {
            if let Err(e) = self.py_operator.call_method0(py, "destroy") {
                e.print(py);
            };
        });
    }

    fn message_cb_event(&mut self, msg: Arc<Message<Vec<u8>>>) -> OperatorEvent {
        let time = msg.timestamp().clone();
        let py_operator = Arc::clone(&self.py_operator);
        let py_time = PyTimestamp::from(time.clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
        let py_write_stream = Arc::clone(&self.py_write_stream);

        OperatorEvent::new(
            time.clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || {
                Python::with_gil(|py| {
                    let erdos = PyModule::import(py, "erdos").unwrap();
                    let pickle = PyModule::import(py, "pickle").unwrap();

                    let context: PyObject = erdos
                        .getattr("context")
                        .unwrap()
                        .getattr("OneInOneOutContext")
                        .unwrap()
                        .call1((
                            py_time,
                            py_operator_config.clone_ref(py),
                            py_write_stream.clone_ref(py),
                        ))
                        .unwrap()
                        .extract()
                        .unwrap();
                    let serialized_data = match msg.data() {
                        Some(d) => PyBytes::new(py, &d[..]),
                        None => unreachable!(),
                    };
                    let py_data: PyObject = pickle
                        .getattr("loads")
                        .unwrap()
                        .call1((serialized_data,))
                        .unwrap()
                        .extract()
                        .unwrap();
                    if let Err(e) = py_operator.call_method1(py, "on_data", (context, py_data)) {
                        e.print(py);
                    };
                })
            },
            OperatorType::Sequential,
        )
    }

    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        let py_operator = Arc::clone(&self.py_operator);
        let py_time = PyTimestamp::from(timestamp.clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
        let py_write_stream = Arc::clone(&self.py_write_stream);
        if self.config.flow_watermarks {
            let mut write_stream_copy = self.write_stream.clone();
            let timestamp_copy = timestamp.clone();
            OperatorEvent::new(
                timestamp.clone(),
                true,
                127,
                HashSet::new(),
                HashSet::new(),
                move || {
                    // Invoke the watermark method.
                    Python::with_gil(|py| {
                        let erdos = PyModule::import(py, "erdos").unwrap();
                        let context: PyObject = erdos
                            .getattr("context")
                            .unwrap()
                            .getattr("OneInOneOutContext")
                            .unwrap()
                            .call1((
                                py_time,
                                py_operator_config.clone_ref(py),
                                py_write_stream.clone_ref(py),
                            ))
                            .unwrap()
                            .extract()
                            .unwrap();
                        if let Err(e) = py_operator.call_method1(py, "on_watermark", (context,)) {
                            e.print(py);
                        };
                    });

                    // Send a watermark.
                    write_stream_copy
                        .send(Message::new_watermark(timestamp_copy))
                        .ok();
                },
                OperatorType::Sequential,
            )
        } else {
            OperatorEvent::new(
                timestamp.clone(),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                move || {
                    Python::with_gil(|py| {
                        let erdos = PyModule::import(py, "erdos").unwrap();
                        let context: PyObject = erdos
                            .getattr("context")
                            .unwrap()
                            .getattr("OneInOneOutContext")
                            .unwrap()
                            .call1((
                                py_time,
                                py_operator_config.clone_ref(py),
                                py_write_stream.clone_ref(py),
                            ))
                            .unwrap()
                            .extract()
                            .unwrap();
                        if let Err(e) = py_operator.call_method1(py, "on_watermark", (context,)) {
                            e.print(py);
                        };
                    });
                },
                OperatorType::Sequential,
            )
        }
    }
}
