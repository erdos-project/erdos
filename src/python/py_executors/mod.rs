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
    OperatorId,
};

// Private submodules.
mod py_source_executor;

// Public exports.
pub use py_source_executor::*;

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
