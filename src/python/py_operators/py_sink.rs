use std::{mem, sync::Arc};

use crate::{
    dataflow::{
        context::SinkContext,
        operator::{OperatorConfig, Sink},
        ReadStream,
    },
    python::{PyReadStream, PyTimestamp},
};
use pyo3::{prelude::*, types::*};

pub(crate) struct PySink {
    py_operator_config: Arc<PyObject>,
    py_operator: Arc<PyObject>,
}

impl PySink {
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

        let py_operator_config_clone = Arc::clone(&py_operator_config);
        let py_operator = super::construct_operator(
            py_operator_type,
            py_operator_args,
            py_operator_kwargs,
            py_operator_config,
            config,
        );

        Self {
            py_operator_config: py_operator_config_clone,
            py_operator,
        }
    }
}

impl Sink<(), Vec<u8>> for PySink {
    fn run(&mut self, read_stream: &mut ReadStream<Vec<u8>>) {
        // Note on the unsafe execution: To conform to the Operator API that passes a mutable
        // reference to the `run` method of an operator, we convert the reference to a raw pointer
        // and create another object that points to the same memory location as the original
        // ReadStream. This is safe since no other part of the OperatorExecutor has access to the
        // ReadStream while `run` is executing.
        // This was required since `PyReadStream` requires ownership of its ReadStream instance
        // since pyo3 does not support annotation of pyclasses with lifetime parameters.
        unsafe {
            // Create another object that points to the same original memory location of the
            // ReadStream.
            let read_stream_ptr: *mut ReadStream<Vec<u8>> = read_stream;
            let read_stream: ReadStream<Vec<u8>> = read_stream_ptr.read();

            // Create the Python version of the ReadStream.
            let read_stream_id = read_stream.id();
            let read_stream_name = String::from(read_stream.name().clone());
            let read_stream_arc = Arc::new(read_stream);
            let py_read_stream = PyReadStream::from(&read_stream_arc);

            // Create the locals to run the constructor for the ReadStream.
            let py_read_stream_obj = Python::with_gil(|py| -> PyObject {
                let locals = PyDict::new(py);
                locals
                    .set_item("py_read_stream", &Py::new(py, py_read_stream).unwrap())
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("read_stream_id", format!("{}", read_stream_id))
                    .err()
                    .map(|e| e.print(py));
                locals
                    .set_item("name", format!("{}", read_stream_name))
                    .err()
                    .map(|e| e.print(py));
                let read_stream_result = py.run(
                    r#"
import uuid, erdos

# Create the ReadStream.
read_stream = erdos.ReadStream(_py_read_stream=py_read_stream,
                               _id=uuid.UUID(read_stream_id))
            "#,
                    None,
                    Some(&locals),
                );
                if let Err(e) = read_stream_result {
                    e.print(py);
                }

                // Retrieve the constructed stream.
                py.eval("read_stream", None, Some(&locals))
                    .unwrap()
                    .to_object(py)
            });

            Python::with_gil(|py| {
                if let Err(e) = self
                    .py_operator
                    .call_method1(py, "run", (py_read_stream_obj,))
                {
                    e.print(py);
                }
            });
            // NOTE: We must forget the Arc<ReadStream> instance since Rust calls the Drop trait
            // at the end of this block, which leads to a dropped ReadStream for the executor, thus
            // preventing the execution of the message and watermark callbacks.
            mem::forget(read_stream_arc);
        }
    }

    fn destroy(&mut self) {
        Python::with_gil(|py| {
            if let Err(e) = self.py_operator.call_method0(py, "destroy") {
                e.print(py);
            };
        });
    }

    fn on_data(&mut self, ctx: &mut SinkContext<()>, data: &Vec<u8>) {
        let py_time = PyTimestamp::from(ctx.get_timestamp().clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
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
            let serialized_data = PyBytes::new(py, &data[..]);
            let py_data: PyObject = pickle
                .getattr("loads")
                .unwrap()
                .call1((serialized_data,))
                .unwrap()
                .extract()
                .unwrap();
            if let Err(e) = self
                .py_operator
                .call_method1(py, "on_data", (context, py_data))
            {
                e.print(py);
            };
        });
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<()>) {
        let py_time = PyTimestamp::from(ctx.get_timestamp().clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
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
            if let Err(e) = self
                .py_operator
                .call_method1(py, "on_watermark", (context,))
            {
                e.print(py);
            };
        });
    }
}
