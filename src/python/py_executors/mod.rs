use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use pyo3::{prelude::*, types::*};
use tokio::{
    self,
    sync::{broadcast, mpsc},
};

use crate::{
    dataflow::{operator::OperatorConfig, Message, ReadStream, StreamT, Timestamp},
    node::{
        lattice::ExecutionLattice,
        operator_event::OperatorEvent,
        operator_executors::OperatorExecutorT,
        worker::{EventNotification, OperatorExecutorNotification, WorkerNotification},
    },
    python::py_stream::PyReadStream,
    OperatorId,
};

// Private submodules.
mod py_one_in_one_out_executor;
mod py_one_in_two_out_executor;
mod py_sink_executor;
mod py_source_executor;

// Public exports.
pub use py_one_in_one_out_executor::*;
pub use py_one_in_two_out_executor::*;
pub use py_sink_executor::*;
pub use py_source_executor::*;

pub struct PyOneInExecutor {
    config: OperatorConfig,
    processor: Box<dyn PyOneInMessageProcessorT>,
    helper: PyOperatorExecutorHelper,
    read_stream: Option<Arc<ReadStream<Vec<u8>>>>,
    py_read_stream: PyObject,
}

impl PyOneInExecutor {
    pub fn new(
        config: OperatorConfig,
        processor: Box<dyn PyOneInMessageProcessorT>,
        read_stream: ReadStream<Vec<u8>>,
    ) -> Self {
        // Create the Python version of the ReadStream.
        let read_stream_id = read_stream.id();
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
                .set_item("name", format!("{}_ReadStream", config.get_name()))
                .err()
                .map(|e| e.print(py));
            let read_stream_result = py.run(
                r#"
import uuid, erdos

# Create the ReadStream.
read_stream = erdos.ReadStream(_py_read_stream=py_read_stream, 
                               _name=name, 
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

        // Retrieve the original stream.
        let operator_id = config.id;
        Self {
            config,
            processor,
            helper: PyOperatorExecutorHelper::new(operator_id),
            read_stream: Some(read_stream_arc),
            py_read_stream: py_read_stream_obj,
        }
    }

    pub(crate) async fn execute(
        &mut self,
        mut channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        // Synchronize the operator with the rest of the dataflow graph.
        self.helper.synchronize().await;

        // TODO (Sukrit): Execute the `setup` method for the operators.
        let mut read_stream = self.read_stream.take().unwrap();

        // Execute the `run` method.
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: Running Operator {}",
            self.config.node_id,
            self.config.get_name()
        );
        tokio::task::block_in_place(|| {
            self.processor.execute_run(&self.py_read_stream);
        });

        // Process messages on the incoming stream.
        unsafe {
            let process_stream_fut = self.helper.process_stream(
                Arc::get_mut_unchecked(&mut read_stream),
                &mut (*self.processor),
                &channel_to_event_runners,
            );

            // Shutdown
            loop {
                tokio::select! {
                    _ = process_stream_fut => break,
                    notification_result = channel_from_worker.recv() => {
                        match notification_result {
                            Ok(notification) => {
                                match notification {
                                    OperatorExecutorNotification::Shutdown => { break; }
                                }
                            }
                            Err(e) => {
                                slog::error!(
                                    crate::TERMINAL_LOGGER,
                                    "PyOneInExecutor {} [{}]: Error receiving notifications {:?}",
                                    self.config.get_name(),
                                    self.operator_id(),
                                    e,
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Invoke the `destroy` method.
        tokio::task::block_in_place(|| self.processor.execute_destroy());

        // Ask the executor to cleanup and notify the worker.
        self.processor.cleanup();
        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl OperatorExecutorT for PyOneInExecutor {
    fn execute<'b>(
        &'b mut self,
        channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) -> Pin<Box<dyn Future<Output = ()> + 'b + Send>> {
        Box::pin(self.execute(
            channel_from_worker,
            channel_to_worker,
            channel_to_event_runners,
        ))
    }

    fn lattice(&self) -> Arc<ExecutionLattice> {
        self.helper.get_lattice()
    }

    fn operator_id(&self) -> OperatorId {
        self.config.id
    }
}

pub trait PyOneInMessageProcessorT: Send + Sync {
    fn execute_run(&mut self, read_stream: &PyObject);

    fn execute_destroy(&mut self);

    fn cleanup(&mut self) {}

    fn message_cb_event(&mut self, msg: Arc<Message<Vec<u8>>>) -> OperatorEvent;

    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent;
}

pub struct PyOperatorExecutorHelper {
    operator_id: OperatorId,
    lattice: Arc<ExecutionLattice>,
}

impl PyOperatorExecutorHelper {
    pub(crate) fn new(operator_id: OperatorId) -> Self {
        Self {
            operator_id,
            lattice: Arc::new(ExecutionLattice::new()),
        }
    }

    pub(crate) fn get_lattice(&self) -> Arc<ExecutionLattice> {
        Arc::clone(&self.lattice)
    }

    pub(crate) async fn synchronize(&self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;
    }

    pub(crate) async fn process_stream(
        &mut self,
        read_stream: &mut ReadStream<Vec<u8>>,
        message_processor: &mut dyn PyOneInMessageProcessorT,
        notifier_tx: &tokio::sync::broadcast::Sender<EventNotification>,
    ) {
        loop {
            tokio::select! {
                // If there is a message on the ReadStream, then increment the messgae counts for
                // the given timestamp, evaluate the start and end condition and install / disarm
                // deadlines accordingly.
                // TODO (Sukrit) : The start and end conditions are evaluated in the thread of the
                // OperatorExecutor, and can be moved to a separate task if they become a
                // bottleneck.
                Ok(msg) = read_stream.async_read() => {
                    let events = match msg.data() {
                        // Data message
                        Some(_) => {
                            let msg_ref = Arc::clone(&msg);
                            let data_event = message_processor.message_cb_event(
                                msg_ref,
                            );

                            vec![data_event]
                        },

                        // Watermark
                        None => {
                            let watermark_event = message_processor.watermark_cb_event(
                                msg.timestamp());
                            vec![watermark_event]
                        }
                    };

                    self.lattice.add_events(events).await;
                    notifier_tx
                        .send(EventNotification::AddedEvents(self.operator_id))
                        .unwrap();
                },
                else => break,
            }
        }
    }
}
