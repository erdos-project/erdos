use std::{mem, sync::Arc};

use erdos::dataflow::{
    context::OneInTwoOutContext,
    operator::{OneInTwoOut, OperatorConfig},
    stream::WriteStreamT,
    Message, ReadStream, WriteStream,
};
use pyo3::{prelude::*, types::*};

use crate::{PyReadStream, PyTimestamp, PyWriteStream};

pub(crate) struct PyOneInTwoOut {
    py_operator_config: Arc<PyObject>,
    py_operator: Arc<PyObject>,
    // The py_write_streams are set to Option, since the constructor does not receive a
    // WriteStream, but we expect this to be populated once the executor calls the `run` method of
    // the operator.
    py_left_write_stream: Option<Arc<PyObject>>,
    py_right_write_stream: Option<Arc<PyObject>>,
}

impl PyOneInTwoOut {
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
            py_left_write_stream: None,
            py_right_write_stream: None,
        }
    }
}

impl OneInTwoOut<(), Vec<u8>, Vec<u8>, Vec<u8>> for PyOneInTwoOut {
    fn run(
        &mut self,
        read_stream: &mut ReadStream<Vec<u8>>,
        left_write_stream: &mut WriteStream<Vec<u8>>,
        right_write_stream: &mut WriteStream<Vec<u8>>,
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
            let read_stream_name = String::from(read_stream.name().clone());
            let read_stream_arc = Arc::new(read_stream);
            let py_read_stream = PyReadStream::from(&read_stream_arc);

            // Create the Python version of the left WriteStream.
            let left_write_stream_clone = left_write_stream.clone();
            let left_write_stream_id = left_write_stream.id();
            let left_write_stream_name = String::from(left_write_stream.name().clone());
            let py_left_write_stream = PyWriteStream::from(left_write_stream_clone);

            // Create the Python version of the right WriteStream.
            let right_write_stream_clone = right_write_stream.clone();
            let right_write_stream_id = right_write_stream.id();
            let right_write_stream_name = String::from(right_write_stream.name().clone());
            let py_right_write_stream = PyWriteStream::from(right_write_stream_clone);

            // Create the locals to run the constructor for the ReadStream and WriteStream.
            let (py_read_stream_obj, py_left_write_stream_obj, py_right_write_stream_obj) =
                Python::with_gil(|py| -> (PyObject, PyObject, PyObject) {
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
                        .set_item("read_stream_name", format!("{}", read_stream_name))
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
                        .set_item("left_write_stream_id", format!("{}", left_write_stream_id))
                        .err()
                        .map(|e| e.print(py));
                    locals
                        .set_item(
                            "left_write_stream_name",
                            format!("{}", left_write_stream_name),
                        )
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
                            "right_write_stream_id",
                            format!("{}", right_write_stream_id),
                        )
                        .err()
                        .map(|e| e.print(py));
                    locals
                        .set_item(
                            "right_write_stream_name",
                            format!("{}", right_write_stream_name),
                        )
                        .err()
                        .map(|e| e.print(py));
                    let stream_construction_result = py.run(
                        r#"
import uuid, erdos

# Create the ReadStream.
read_stream = erdos.ReadStream(_py_read_stream=py_read_stream)

# Create the left WriteStream.
left_write_stream = erdos.WriteStream(_py_write_stream=py_left_write_stream)

# Create the right WriteStream.
right_write_stream = erdos.WriteStream(_py_write_stream=py_right_write_stream)
            "#,
                        None,
                        Some(&locals),
                    );
                    if let Err(e) = stream_construction_result {
                        e.print(py);
                    }

                    // Retrieve the constructed streams.
                    let py_read_stream_obj = py
                        .eval("read_stream", None, Some(&locals))
                        .unwrap()
                        .to_object(py);
                    let py_left_write_stream_obj = py
                        .eval("left_write_stream", None, Some(&locals))
                        .unwrap()
                        .to_object(py);
                    let py_right_write_stream_obj = py
                        .eval("right_write_stream", None, Some(&locals))
                        .unwrap()
                        .to_object(py);

                    (
                        py_read_stream_obj,
                        py_left_write_stream_obj,
                        py_right_write_stream_obj,
                    )
                });

            // Save the constructed WriteStreams, and invoke the `run` method.
            let py_left_write_stream_arc = Arc::new(py_left_write_stream_obj);
            let py_left_write_stream_arc_clone = Arc::clone(&py_left_write_stream_arc);
            self.py_left_write_stream = Some(py_left_write_stream_arc);

            let py_right_write_stream_arc = Arc::new(py_right_write_stream_obj);
            let py_right_write_stream_arc_clone = Arc::clone(&py_right_write_stream_arc);
            self.py_right_write_stream = Some(py_right_write_stream_arc);

            Python::with_gil(|py| {
                if let Err(e) = self.py_operator.call_method1(
                    py,
                    "run",
                    (
                        py_read_stream_obj,
                        py_left_write_stream_arc_clone.clone_ref(py),
                        py_right_write_stream_arc_clone.clone_ref(py),
                    ),
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

    fn on_data(&mut self, ctx: &mut OneInTwoOutContext<(), Vec<u8>, Vec<u8>>, data: &Vec<u8>) {
        let py_time = PyTimestamp::from(ctx.get_timestamp().clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
        let py_left_write_stream = match &self.py_left_write_stream {
            Some(d) => Arc::clone(d),
            None => unreachable!(),
        };
        let py_right_write_stream = match &self.py_right_write_stream {
            Some(d) => Arc::clone(d),
            None => unreachable!(),
        };

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

    fn on_watermark(&mut self, ctx: &mut OneInTwoOutContext<(), Vec<u8>, Vec<u8>>) {
        let py_time = PyTimestamp::from(ctx.get_timestamp().clone());
        let py_operator_config = Arc::clone(&self.py_operator_config);
        let py_left_write_stream = match &self.py_left_write_stream {
            Some(d) => Arc::clone(d),
            None => unreachable!(),
        };
        let py_right_write_stream = match &self.py_right_write_stream {
            Some(d) => Arc::clone(d),
            None => unreachable!(),
        };

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
            if let Err(e) = self
                .py_operator
                .call_method1(py, "on_watermark", (context,))
            {
                e.print(py);
            };
        });

        // Send a watermark if flow_watermarks is set in the OperatorConfig.
        if ctx.get_operator_config().flow_watermarks {
            let left_timestamp = ctx.get_timestamp().clone();
            let right_timestamp = ctx.get_timestamp().clone();
            ctx.get_left_write_stream()
                .send(Message::new_watermark(left_timestamp))
                .ok();
            ctx.get_right_write_stream()
                .send(Message::new_watermark(right_timestamp))
                .ok();
        }
    }
}
