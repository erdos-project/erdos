use std::{mem, sync::Arc};

use erdos::dataflow::{
    context::OneInOneOutContext,
    operator::{OneInOneOut, OperatorConfig},
    stream::WriteStreamT,
    Message, ReadStream, WriteStream,
};
use pyo3::{prelude::*, types::*};

use crate::{PyReadStream, PyTimestamp, PyWriteStream};

pub(crate) struct PyOneInOneOut {
    py_operator_config: Arc<PyObject>,
    py_operator: Arc<PyObject>,
    // The py_write_stream is set to Option, since the constructor does not receive a WriteStream,
    // but we expect this to be populated once the executor calls the `run` method of the operator.
    py_write_stream: Option<Arc<PyObject>>,
}

impl PyOneInOneOut {
    pub(crate) fn new(
        py_operator_type: Arc<PyObject>,
        py_operator_args: Arc<PyObject>,
        py_operator_kwargs: Arc<PyObject>,
        py_operator_config: Arc<PyObject>,
        config: OperatorConfig,
    ) -> Self {
        // Instantiate the Operator in Python.
        tracing::debug!("Instantiating the operator {:?}", config.name);

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
            py_write_stream: None,
        }
    }
}

impl OneInOneOut<(), Vec<u8>, Vec<u8>> for PyOneInOneOut {
    fn run(
        &mut self,
        _config: &OperatorConfig,
        read_stream: &mut ReadStream<Vec<u8>>,
        write_stream: &mut WriteStream<Vec<u8>>,
    ) {
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
            let read_stream_name = read_stream.name();
            let read_stream_arc = Arc::new(read_stream);
            let py_read_stream = PyReadStream::from(&read_stream_arc);

            // Create the Python version of the WriteStream.
            let write_stream_clone = write_stream.clone();
            let write_stream_id = write_stream.id();
            let write_stream_name = write_stream.name();
            let py_write_stream = PyWriteStream::from(write_stream_clone);

            // Create the locals to run the constructor for the ReadStream and WriteStream.
            let (py_read_stream_obj, py_write_stream_obj) =
                Python::with_gil(|py| -> (PyObject, PyObject) {
                    let locals = PyDict::new(py);
                    if let Some(e) = locals
                        .set_item("py_read_stream", &Py::new(py, py_read_stream).unwrap())
                        .err()
                    {
                        e.print(py)
                    }
                    if let Some(e) = locals
                        .set_item("read_stream_id", format!("{}", read_stream_id))
                        .err()
                    {
                        e.print(py)
                    }
                    if let Some(e) = locals
                        .set_item("read_stream_name", read_stream_name.to_string())
                        .err()
                    {
                        e.print(py)
                    }
                    if let Some(e) = locals
                        .set_item("py_write_stream", &Py::new(py, py_write_stream).unwrap())
                        .err()
                    {
                        e.print(py)
                    }
                    if let Some(e) = locals
                        .set_item("write_stream_id", format!("{}", write_stream_id))
                        .err()
                    {
                        e.print(py)
                    }
                    if let Some(e) = locals
                        .set_item("write_stream_name", write_stream_name.to_string())
                        .err()
                    {
                        e.print(py)
                    }
                    let stream_construction_result = py.run(
                        r#"
import uuid, erdos

# Create the ReadStream.
read_stream = erdos.ReadStream(_py_read_stream=py_read_stream)

# Create the WriteStream.
write_stream = erdos.WriteStream(_py_write_stream=py_write_stream)
            "#,
                        None,
                        Some(locals),
                    );
                    if let Err(e) = stream_construction_result {
                        e.print(py);
                    }

                    // Retrieve the constructed stream.
                    let py_read_stream_obj = py
                        .eval("read_stream", None, Some(locals))
                        .unwrap()
                        .to_object(py);
                    let py_write_stream_obj = py
                        .eval("write_stream", None, Some(locals))
                        .unwrap()
                        .to_object(py);

                    (py_read_stream_obj, py_write_stream_obj)
                });

            // Save the constructed WriteStream, and invoke the `run` method.
            let py_write_stream_arc = Arc::new(py_write_stream_obj);
            let py_write_stream_arc_clone = Arc::clone(&py_write_stream_arc);
            self.py_write_stream = Some(py_write_stream_arc);
            Python::with_gil(|py| {
                if let Err(e) = self.py_operator.call_method1(
                    py,
                    "run",
                    (py_read_stream_obj, py_write_stream_arc_clone.clone_ref(py)),
                ) {
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
            }
        });
    }

    fn on_data(&mut self, ctx: &mut OneInOneOutContext<(), Vec<u8>>, data: &Vec<u8>) {
        let py_time = PyTimestamp::from(ctx.timestamp().clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
        let py_write_stream = match &self.py_write_stream {
            Some(d) => Arc::clone(d),
            None => unreachable!(),
        };
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

    fn on_watermark(&mut self, ctx: &mut OneInOneOutContext<(), Vec<u8>>) {
        let py_time = PyTimestamp::from(ctx.timestamp().clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
        let py_write_stream = match &self.py_write_stream {
            Some(d) => Arc::clone(d),
            None => unreachable!(),
        };

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
            if let Err(e) = self
                .py_operator
                .call_method1(py, "on_watermark", (context,))
            {
                e.print(py);
            };
        });

        // Send a watermark if flow_watermarks is set in the OperatorConfig.
        if ctx.operator_config().flow_watermarks {
            let timestamp = ctx.timestamp().clone();
            ctx.write_stream()
                .send(Message::new_watermark(timestamp))
                .ok();
        }
    }
}
