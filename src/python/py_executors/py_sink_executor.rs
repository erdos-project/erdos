use std::{collections::HashSet, sync::Arc};

use crate::{
    dataflow::{operator::OperatorConfig, Message, Timestamp},
    node::operator_event::{OperatorEvent, OperatorType},
    python::{py_executors::PyOneInMessageProcessorT, PyTimestamp},
};
use pyo3::{prelude::*, types::*};

pub struct PySinkMessageProcessor {
    py_operator_config: Arc<PyObject>,
    py_operator: Arc<PyObject>,
}

impl PySinkMessageProcessor {
    pub(crate) fn new(
        py_operator_type: Arc<PyObject>,
        py_operator_args: Arc<PyObject>,
        py_operator_kwargs: Arc<PyObject>,
        py_operator_config: Arc<PyObject>,
        config: OperatorConfig,
    ) -> Self {
        // Instantiate the operator in Python.
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Instantiating the operator {:?}",
            config.name
        );

        // Create the locals to run the constructor.
        let py_operator = Python::with_gil(|py| -> PyObject {
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

            // Initialize the operator.
            let init_result = py.run(
                r#"
import uuid, erdos

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
            py.eval("operator", None, Some(&locals))
                .unwrap()
                .to_object(py)
        });
        Self {
            py_operator_config,
            py_operator: Arc::new(py_operator),
        }
    }
}

impl PyOneInMessageProcessorT for PySinkMessageProcessor {
    fn execute_run(&mut self, read_stream: &PyObject) {
        Python::with_gil(|py| {
            if let Err(e) = self.py_operator.call_method1(py, "run", (read_stream,)) {
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
                        .getattr("SinkContext")
                        .unwrap()
                        .call1((py_time, py_operator_config.clone_ref(py)))
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
                });
            },
            OperatorType::Sequential,
        )
    }

    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        let py_operator = Arc::clone(&self.py_operator);
        let py_time = PyTimestamp::from(timestamp.clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
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
                        .getattr("SinkContext")
                        .unwrap()
                        .call1((py_time, py_operator_config.clone_ref(py)))
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
