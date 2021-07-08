use tokio::{
    self,
    sync::{broadcast, mpsc},
};

use crate::{
    dataflow::operator::OperatorConfig,
    node::{
        operator_executors::OperatorExecutorHelper,
        worker::{EventNotification, OperatorExecutorNotification, WorkerNotification},
    },
};

pub trait MessageProcessorT {
    fn run(&self);
}

pub struct OperatorExecutor<'a> {
    config: OperatorConfig,
    helper: Option<OperatorExecutorHelper>,
    message_processor: &'a dyn MessageProcessorT,
}

impl<'a> OperatorExecutor<'a> {
    pub fn new<T, U>(config: OperatorConfig, message_processor: &'a dyn MessageProcessorT) -> Self {
        let operator_id = config.id;
        Self {
            config,
            helper: Some(OperatorExecutorHelper::new(operator_id)),
            message_processor,
        }
    }

    pub(crate) async fn execute(
        &mut self,
        mut channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        // Synchronize the operator with the remainder of the graph.
        let mut helper = self.helper.take().unwrap();
        helper.synchronize().await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: Running Operator {}",
            self.config.node_id,
            self.config.get_name(),
        );

        // Execute the `run` method.
        tokio::task::block_in_place(|| self.message_processor.run());
    }
}
