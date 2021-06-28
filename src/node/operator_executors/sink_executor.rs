use serde::Deserialize;
use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};
use tokio::{
    self,
    sync::{broadcast, mpsc, Mutex},
};

use crate::{
    dataflow::{
        operator::{
            OneInOneOutSetupContext, OperatorConfig, Sink, SinkContext, StatefulSinkContext,
        },
        Data, Message, ReadStream, State, StreamT, Timestamp,
    },
    node::{
        lattice::ExecutionLattice,
        operator_event::OperatorEvent,
        operator_executors::{OneInMessageProcessorT, OperatorExecutorHelper, OperatorExecutorT},
        worker::{EventNotification, OperatorExecutorNotification, WorkerNotification},
    },
    OperatorId, Uuid,
};

pub struct SinkExecutor<O, S, T>
where
    O: Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: O,
    state: Arc<Mutex<S>>,
    read_stream: Option<ReadStream<T>>,
    helper: Option<OperatorExecutorHelper>,
    state_ids: HashSet<Uuid>,
}

impl<O, S, T> SinkExecutor<O, S, T>
where
    O: Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        read_stream: ReadStream<T>,
    ) -> Self {
        let operator_id = config.id;
        Self {
            config,
            operator: operator_fn(),
            state: Arc::new(Mutex::new(state_fn())),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            read_stream: Some(read_stream),
            helper: Some(OperatorExecutorHelper::new(operator_id)),
        }
    }

    pub(crate) async fn execute(
        &mut self,
        mut channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        let mut helper = self.helper.take().unwrap();
        helper.synchronize().await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| self.operator.run(self.read_stream.as_mut().unwrap()));

        // Run the setup method.
        let setup_context = OneInOneOutSetupContext::new(self.read_stream.as_ref().unwrap().id());

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        let process_stream_fut = helper.process_stream(
            read_stream,
            &(*self),
            &channel_to_event_runners,
            &setup_context,
        );
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
                            slog::error!(crate::get_terminal_logger(),
                            "Operator executor {}: error receiving notifications {:?}", self.operator_id(), e);
                            break;
                        }
                    }
                }
            };
        }

        tokio::task::block_in_place(|| self.operator.destroy());

        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl<O, S, T> OperatorExecutorT for SinkExecutor<O, S, T>
where
    O: Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
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
        Arc::clone(&self.helper.as_ref().unwrap().lattice)
    }

    fn operator_id(&self) -> OperatorId {
        self.config.id
    }
}

impl<O, S, T> OneInMessageProcessorT<T> for SinkExecutor<O, S, T>
where
    O: Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    fn stateless_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = SinkContext {
            timestamp: msg.timestamp().clone(),
            config: self.config.clone(),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || O::on_data(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a stateful callback.
    fn stateful_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = StatefulSinkContext {
            timestamp: msg.timestamp().clone(),
            config: self.config.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || O::on_data_stateful(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&self, timestamp: &Timestamp) -> OperatorEvent {
        let mut ctx = StatefulSinkContext {
            timestamp: timestamp.clone(),
            config: self.config.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            timestamp.clone(),
            true,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || O::on_watermark(&mut ctx),
        )
    }
}
