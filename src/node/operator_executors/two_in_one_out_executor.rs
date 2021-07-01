use serde::Deserialize;
use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    self,
    sync::{broadcast, mpsc},
};

use crate::{
    dataflow::{
        operator::{OperatorConfig, StatefulTwoInOneOutContext, TwoInOneOut, TwoInOneOutContext},
        stream::WriteStreamT,
        Data, Message, ReadStream, State, Timestamp, WriteStream,
    },
    node::{
        lattice::ExecutionLattice,
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::{OperatorExecutorHelper, OperatorExecutorT, TwoInMessageProcessorT},
        worker::{EventNotification, OperatorExecutorNotification, WorkerNotification},
    },
    OperatorId, Uuid,
};

pub struct TwoInOneOutExecutor<O, S, T, U, V>
where
    O: TwoInOneOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: O,
    state: Arc<Mutex<S>>,
    left_read_stream: Option<ReadStream<T>>,
    right_read_stream: Option<ReadStream<U>>,
    write_stream: WriteStream<V>,
    state_ids: HashSet<Uuid>,
    helper: Option<OperatorExecutorHelper>,
}

impl<O, S, T, U, V> TwoInOneOutExecutor<O, S, T, U, V>
where
    O: TwoInOneOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        left_read_stream: ReadStream<T>,
        right_read_stream: ReadStream<U>,
        write_stream: WriteStream<V>,
    ) -> Self {
        let operator_id = config.id;
        Self {
            config,
            operator: operator_fn(),
            state: Arc::new(Mutex::new(state_fn())),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            left_read_stream: Some(left_read_stream),
            right_read_stream: Some(right_read_stream),
            write_stream,
            helper: Some(OperatorExecutorHelper::new(operator_id)),
        }
    }

    pub(crate) async fn execute(
        &mut self,
        mut channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        let helper = self.helper.take().unwrap();
        helper.synchronize().await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| {
            self.operator.run(
                self.left_read_stream.as_mut().unwrap(),
                self.right_read_stream.as_mut().unwrap(),
                &mut self.write_stream,
            )
        });

        let left_read_stream: ReadStream<T> = self.left_read_stream.take().unwrap();
        let right_read_stream: ReadStream<U> = self.right_read_stream.take().unwrap();
        let process_stream_fut = helper.process_two_streams(
            left_read_stream,
            right_read_stream,
            &(*self),
            &channel_to_event_runners,
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

        // Return the helper.
        self.helper.replace(helper);

        // Close the stream.
        // TODO: check that the top watermark hasn't already been sent.
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

impl<O, S, T, U, V> OperatorExecutorT for TwoInOneOutExecutor<O, S, T, U, V>
where
    O: TwoInOneOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
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

impl<O, S, T, U, V> TwoInMessageProcessorT<T, U> for TwoInOneOutExecutor<O, S, T, U, V>
where
    O: TwoInOneOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    // Generates an OperatorEvent for a stateless callback on the first stream.
    fn left_stateless_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = TwoInOneOutContext {
            timestamp: msg.timestamp().clone(),
            config: self.config.clone(),
            write_stream: self.write_stream.clone(),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || O::on_left_data(&mut ctx, msg.data().unwrap()),
            OperatorType::ReadOnly,
        )
    }

    // Generates an OperatorEvent for a stateful callback on the first stream.
    fn left_stateful_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = StatefulTwoInOneOutContext {
            timestamp: msg.timestamp().clone(),
            config: self.config.clone(),
            write_stream: self.write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || O::on_left_data_stateful(&mut ctx, msg.data().unwrap()),
            OperatorType::ReadOnly,
        )
    }

    // Generates an OperatorEvent for a stateless callback on the second stream.
    fn right_stateless_cb_event(&self, msg: Arc<Message<U>>) -> OperatorEvent {
        let mut ctx = TwoInOneOutContext {
            timestamp: msg.timestamp().clone(),
            config: self.config.clone(),
            write_stream: self.write_stream.clone(),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || O::on_right_data(&mut ctx, msg.data().unwrap()),
            OperatorType::ReadOnly,
        )
    }

    // Generates an OperatorEvent for a stateful callback on the second stream.
    fn right_stateful_cb_event(&self, msg: Arc<Message<U>>) -> OperatorEvent {
        let mut ctx = StatefulTwoInOneOutContext {
            timestamp: msg.timestamp().clone(),
            config: self.config.clone(),
            write_stream: self.write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || O::on_right_data_stateful(&mut ctx, msg.data().unwrap()),
            OperatorType::ReadOnly,
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&self, timestamp: &Timestamp) -> OperatorEvent {
        let mut ctx = StatefulTwoInOneOutContext {
            timestamp: timestamp.clone(),
            config: self.config.clone(),
            write_stream: self.write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        if self.config.flow_watermarks {
            let timestamp_copy = timestamp.clone();
            let mut write_stream_copy = self.write_stream.clone();
            OperatorEvent::new(
                timestamp.clone(),
                true,
                127,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    O::on_watermark(&mut ctx);
                    write_stream_copy
                        .send(Message::new_watermark(timestamp_copy))
                        .ok();
                },
                OperatorType::ReadOnly,
            )
        } else {
            OperatorEvent::new(
                timestamp.clone(),
                true,
                0,
                HashSet::new(),
                self.state_ids.clone(),
                move || O::on_watermark(&mut ctx),
                OperatorType::ReadOnly,
            )
        }
    }
}
