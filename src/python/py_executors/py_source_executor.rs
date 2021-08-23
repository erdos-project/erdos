use std::{future::Future, pin::Pin, sync::Arc};

use pyo3::{prelude::*, types::*};
use tokio::{
    self,
    sync::{broadcast, mpsc},
};

use crate::{
    dataflow::{
        operator::OperatorConfig, stream::WriteStreamT, Message, StreamT, Timestamp, WriteStream,
    },
    node::{
        lattice::ExecutionLattice,
        operator_executors::OperatorExecutorT,
        worker::{EventNotification, OperatorExecutorNotification, WorkerNotification},
    },
    python::{py_executors::PyOperatorExecutorHelper, py_stream::PyWriteStream},
    OperatorId,
};

pub struct PySourceExecutor {
    config: OperatorConfig,
    helper: PyOperatorExecutorHelper,
    write_stream: WriteStream<Vec<u8>>,
    py_operator: PyObject,
    py_write_stream: PyObject,
}

impl PySourceExecutor {
    pub fn new(
        py_operator_type: Arc<PyObject>,
        py_operator_args: Arc<PyObject>,
        py_operator_kwargs: Arc<PyObject>,
        py_operator_config: Arc<PyObject>,
        config: OperatorConfig,
        write_stream: WriteStream<Vec<u8>>,
    ) -> Self {
        // Create the locals to run the constructor and initialize the write stream.
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Instantiating the operator {:?} and the WriteStream {:?}",
            config.name,
            write_stream.name(),
        );

        // Construct the Python version of the WriteStream.
        let write_stream_clone = write_stream.clone();
        let write_stream_id = write_stream.id().clone();
        let py_write_stream = PyWriteStream::from(write_stream);

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
            let py_operator = py
                .eval("operator", None, Some(&locals))
                .unwrap()
                .to_object(py);
            let py_write_stream_obj = py
                .eval("write_stream", None, Some(&locals))
                .unwrap()
                .to_object(py);
            (py_operator, py_write_stream_obj)
        });

        let operator_id = config.id;
        Self {
            config,
            helper: PyOperatorExecutorHelper::new(operator_id),
            write_stream: write_stream_clone,
            py_operator: py_operator,
            py_write_stream: py_write_stream_obj,
        }
    }

    pub(crate) async fn execute(
        &mut self,
        _channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        _channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        // Synchronize the operator with the rest of the dataflow graph.
        self.helper.synchronize().await;

        // Execute the `run` method.
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: Running Operator {}",
            self.config.node_id,
            self.config.get_name(),
        );

        tokio::task::block_in_place(|| {
            Python::with_gil(|py| {
                if let Err(e) = self
                    .py_operator
                    .call_method1(py, "run", (&self.py_write_stream,))
                {
                    e.print(py);
                };
            });
        });

        // Execute the `destroy` method.
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: Destroying Operator {}",
            self.config.node_id,
            self.config.get_name(),
        );

        tokio::task::block_in_place(|| {
            Python::with_gil(|py| {
                if let Err(e) = self.py_operator.call_method0(py, "destroy") {
                    e.print(py);
                };
            });
        });

        // Close the stream.
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .unwrap();
        }

        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl OperatorExecutorT for PySourceExecutor {
    fn execute<'a>(
        &'a mut self,
        channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute(
            channel_from_worker,
            channel_to_worker,
            channel_to_event_runners,
        ))
    }

    fn lattice(&self) -> Arc<ExecutionLattice> {
        Arc::clone(&self.helper.lattice)
    }

    fn operator_id(&self) -> OperatorId {
        self.config.id
    }
}
