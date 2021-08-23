use std::{collections::HashSet, sync::Arc};

use crate::{
    dataflow::{
        operator::OperatorConfig, stream::WriteStreamT, Message, StreamT, Timestamp, WriteStream,
    },
    node::operator_event::{OperatorEvent, OperatorType},
    python::{py_executors::PyOneInMessageProcessorT, py_stream::PyWriteStream, PyTimestamp},
};

use pyo3::{prelude::*, types::*};

pub struct PyOneInTwoOutMessageProcessor {
    py_operator_config: Arc<PyObject>,
    py_operator: Arc<PyObject>,
    py_left_write_stream: Arc<PyObject>,
    py_right_write_stream: Arc<PyObject>,
    left_write_stream: WriteStream<Vec<u8>>,
    right_write_stream: WriteStream<Vec<u8>>,
    config: OperatorConfig,
}

impl PyOneInTwoOutMessageProcessor {
    pub(crate) fn new(
        py_operator_type: Arc<PyObject>,
        py_operator_args: Arc<PyObject>,
        py_operator_kwargs: Arc<PyObject>,
        py_operator_config: Arc<PyObject>,
        config: OperatorConfig,
        left_write_stream: WriteStream<Vec<u8>>,
        right_write_stream: WriteStream<Vec<u8>>,
    ) -> Self {
        // Construct the Python version of the WriteStream.
        let left_write_stream_clone = left_write_stream.clone();
        let right_write_stream_clone = right_write_stream.clone();
        let left_write_stream_id = left_write_stream.id().clone();
        let right_write_stream_id = right_write_stream.id().clone();
        let py_left_write_stream = PyWriteStream::from(left_write_stream);
        let py_right_write_stream = PyWriteStream::from(right_write_stream);

        // Instantiate the operator in Python.
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Instantiating the operator {:?}",
            config.name
        );

        // Create the locals to run the constructor.
        let (py_operator, py_left_write_stream_obj, py_right_write_stream_obj) =
            Python::with_gil(|py| -> (PyObject, PyObject, PyObject) {
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
                    .set_item(
                        "py_left_write_stream",
                        &Py::new(py, py_left_write_stream).unwrap(),
                    )
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item(
                        "left_write_stream_name",
                        format!("{}_Left_WriteStream", config.get_name()),
                    )
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("left_write_stream_id", format!("{}", left_write_stream_id))
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item(
                        "py_right_write_stream",
                        &Py::new(py, py_right_write_stream).unwrap(),
                    )
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item(
                        "right_write_stream_name",
                        format!("{}_Right_WriteStream", config.get_name()),
                    )
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item(
                        "right_write_stream_id",
                        format!("{}", right_write_stream_id),
                    )
                    .err()
                    .map(|e| e.print(py));

                // Initialize the operator.
                let init_result = py.run(
                    r#"
import uuid, erdos

# Create the WriteStream.
left_write_stream = erdos.WriteStream(
    _py_write_stream=py_left_write_stream,
    _name=left_write_stream_name,
    _id=uuid.UUID(left_write_stream_id),
)

right_write_stream = erdos.WriteStream(
    _py_write_stream=py_right_write_stream,
    _name=right_write_stream_name,
    _id=uuid.UUID(right_write_stream_id),
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

                // Retrive the constructed WriteStreams.
                let py_left_write_stream_obj = py
                    .eval("left_write_stream", None, Some(&locals))
                    .unwrap()
                    .to_object(py);
                let py_right_write_stream_obj = py
                    .eval("right_write_stream", None, Some(&locals))
                    .unwrap()
                    .to_object(py);

                (
                    py_operator,
                    py_left_write_stream_obj,
                    py_right_write_stream_obj,
                )
            });
        Self {
            py_operator_config,
            py_operator: Arc::new(py_operator),
            py_left_write_stream: Arc::new(py_left_write_stream_obj),
            py_right_write_stream: Arc::new(py_right_write_stream_obj),
            left_write_stream: left_write_stream_clone,
            right_write_stream: right_write_stream_clone,
            config,
        }
    }
}

impl PyOneInMessageProcessorT for PyOneInTwoOutMessageProcessor {
    fn execute_run(&mut self, read_stream: &PyObject) {
        let py_left_write_stream = Arc::clone(&self.py_left_write_stream);
        let py_right_write_stream = Arc::clone(&self.py_right_write_stream);
        Python::with_gil(|py| {
            if let Err(e) = self.py_operator.call_method1(
                py,
                "run",
                (
                    read_stream,
                    py_left_write_stream.clone_ref(py),
                    py_right_write_stream.clone_ref(py),
                ),
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
        let py_left_write_stream = Arc::clone(&self.py_left_write_stream);
        let py_right_write_stream = Arc::clone(&self.py_right_write_stream);

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
                        .getattr("OneInTwoOutContext")
                        .unwrap()
                        .call1((
                            py_time,
                            py_operator_config.clone_ref(py),
                            py_left_write_stream.clone_ref(py),
                            py_right_write_stream.clone_ref(py),
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
        let py_left_write_stream = Arc::clone(&self.py_left_write_stream);
        let py_right_write_stream = Arc::clone(&self.py_right_write_stream);
        if self.config.flow_watermarks {
            let mut left_write_stream_copy = self.left_write_stream.clone();
            let mut right_write_stream_copy = self.right_write_stream.clone();
            let timestamp_copy_left = timestamp.clone();
            let timestamp_copy_right = timestamp.clone();
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
                            .getattr("OneInTwoOutContext")
                            .unwrap()
                            .call1((
                                py_time,
                                py_operator_config.clone_ref(py),
                                py_left_write_stream.clone_ref(py),
                                py_right_write_stream.clone_ref(py),
                            ))
                            .unwrap()
                            .extract()
                            .unwrap();
                        if let Err(e) = py_operator.call_method1(py, "on_watermark", (context,)) {
                            e.print(py);
                        };
                    });

                    // Send a watermark.
                    left_write_stream_copy
                        .send(Message::new_watermark(timestamp_copy_left))
                        .ok();
                    right_write_stream_copy
                        .send(Message::new_watermark(timestamp_copy_right))
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
                            .getattr("OneInTwoOutContext")
                            .unwrap()
                            .call1((
                                py_time,
                                py_operator_config.clone_ref(py),
                                py_left_write_stream.clone_ref(py),
                                py_right_write_stream.clone_ref(py),
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
