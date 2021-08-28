use std::sync::Arc;

use crate::{
    dataflow::{
        operator::{OperatorConfig, Source},
        StreamT, WriteStream,
    },
    python::PyWriteStream,
};
use pyo3::{prelude::*, types::*};

pub(crate) struct PySource {
    py_operator: Arc<PyObject>,
}

impl PySource {
    pub(crate) fn new(
        py_operator_type: Arc<PyObject>,
        py_operator_args: Arc<PyObject>,
        py_operator_kwargs: Arc<PyObject>,
        py_operator_config: Arc<PyObject>,
        config: OperatorConfig,
    ) -> Self {
        // Instantiate the Operator in Python.
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
            py_operator: Arc::new(py_operator),
        }
    }
}

impl Source<(), Vec<u8>> for PySource {
    fn run(&mut self, write_stream: &mut WriteStream<Vec<u8>>) {
        // Create the Python version of the WriteStream.
        let write_stream_clone = write_stream.clone();
        let write_stream_id = write_stream.id();
        let write_stream_name = String::from(write_stream.name().clone());
        let py_write_stream = PyWriteStream::from(write_stream_clone);

        // Invoke the `run` method.
        Python::with_gil(|py| {
            let locals = PyDict::new(py);
            locals
                .set_item("py_write_stream", &Py::new(py, py_write_stream).unwrap())
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("write_stream_id", format!("{}", write_stream_id))
                .err()
                .map(|e| e.print(py));
            locals
                .set_item("write_stream_name", format!("{}", write_stream_name))
                .err()
                .map(|e| e.print(py));
            let stream_construction_result = py.run(
                r#"
import uuid, erdos

# Create the WriteStream.
write_stream = erdos.WriteStream(_py_write_stream=py_write_stream,
                                 _name=write_stream_name,
                                 _id=uuid.UUID(write_stream_id))
            "#,
                None,
                Some(&locals),
            );
            if let Err(e) = stream_construction_result {
                e.print(py);
            }

            // Retrieve the constructed stream.
            let py_write_stream_obj = py
                .eval("write_stream", None, Some(&locals))
                .unwrap()
                .to_object(py);

            // Invoke the `run` method.
            if let Err(e) = self
                .py_operator
                .call_method1(py, "run", (py_write_stream_obj,))
            {
                e.print(py);
            }
        });
    }

    fn destroy(&mut self) {
        Python::with_gil(|py| {
            if let Err(e) = self.py_operator.call_method0(py, "destroy") {
                e.print(py);
            }
        });
    }
}
