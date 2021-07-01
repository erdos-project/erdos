use serde::Deserialize;
use std::{
    collections::HashSet,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    self,
    sync::{broadcast, mpsc},
};

use crate::{
    dataflow::{
        operator::{
            OneInOneOutSetupContext, OperatorConfig, ReadOnlySink, ReadOnlySinkContext, Sink,
            SinkContext, StatefulSinkContext, WriteableSink, WriteableSinkContext,
        },
        Data, Message, ReadOnlyState, ReadStream, State, StreamT, Timestamp, WriteableState,
    },
    node::{
        lattice::ExecutionLattice,
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::{OneInMessageProcessorT, OperatorExecutorHelper, OperatorExecutorT},
        worker::{EventNotification, OperatorExecutorNotification, WorkerNotification},
    },
    OperatorId, Uuid,
};

/*************************************************************************************************
 * ReadOnlySinkExecutor                                                                          *
 ************************************************************************************************/

pub struct ReadOnlySinkExecutor<O, S, T, U, V>
where
    O: 'static + ReadOnlySink<S, T, U, V>,
    S: ReadOnlyState<U, V>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    config: OperatorConfig,
    operator: Arc<O>,
    state: Arc<S>,
    read_stream: Option<ReadStream<T>>,
    helper: Option<OperatorExecutorHelper>,
    state_ids: HashSet<Uuid>,
    phantom_u: PhantomData<U>,
    phantom_v: PhantomData<V>,
}

impl<O, S, T, U, V> ReadOnlySinkExecutor<O, S, T, U, V>
where
    O: 'static + ReadOnlySink<S, T, U, V>,
    S: ReadOnlyState<U, V>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
    V: 'static + Send + Sync,
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
            operator: Arc::new(operator_fn()),
            state: Arc::new(state_fn()),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            read_stream: Some(read_stream),
            helper: Some(OperatorExecutorHelper::new(operator_id)),
            phantom_u: PhantomData,
            phantom_v: PhantomData,
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
            "Node {}: Running Operator {}",
            self.config.node_id,
            self.config.get_name(),
        );

        tokio::task::block_in_place(|| {
            Arc::get_mut(&mut self.operator)
                .unwrap()
                .run(self.read_stream.as_mut().unwrap())
        });

        // Run the setup method.
        let setup_context = OneInOneOutSetupContext::new(self.read_stream.as_ref().unwrap().id());

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        let process_stream_fut = helper.process_stream(
            read_stream,
            &mut (*self),
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
                            slog::error!(
                                crate::get_terminal_logger(),
                                "Operator executor {}: error receiving notifications {:?}",
                                self.operator_id(),
                                e,
                            );
                            break;
                        }
                    }
                }
            };
        }

        tokio::task::block_in_place(|| Arc::get_mut(&mut self.operator).unwrap().destroy());

        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl<O, S, T, U, V> OperatorExecutorT for ReadOnlySinkExecutor<O, S, T, U, V>
where
    O: ReadOnlySink<S, T, U, V>,
    S: ReadOnlyState<U, V>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
    V: 'static + Send + Sync,
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

impl<O, S, T, U, V> OneInMessageProcessorT<T> for ReadOnlySinkExecutor<O, S, T, U, V>
where
    O: 'static + ReadOnlySink<S, T, U, V>,
    S: ReadOnlyState<U, V>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    // Generates an OperatorEvent for a message callback.
    fn message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
        // Clone the reference to the operator and the state.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let time = msg.timestamp().clone();
        let config = self.config.clone();

        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || {
                operator.on_data(
                    &ReadOnlySinkContext::new(time, config, &state),
                    msg.data().unwrap(),
                )
            },
            OperatorType::ReadOnly,
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        // Clone the reference to the operator and the state.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let time = timestamp.clone();

        OperatorEvent::new(
            timestamp.clone(),
            true,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || operator.on_watermark(&mut ReadOnlySinkContext::new(time, config, &state)),
            OperatorType::ReadOnly,
        )
    }
}

/*************************************************************************************************
 * WriteableSinkExecutor                                                                         *
 ************************************************************************************************/

pub struct WriteableSinkExecutor<O, S, T, U>
where
    O: 'static + WriteableSink<S, T, U>,
    S: WriteableState<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
{
    config: OperatorConfig,
    operator: Arc<Mutex<O>>,
    state: Arc<S>,
    read_stream: Option<ReadStream<T>>,
    helper: Option<OperatorExecutorHelper>,
    state_ids: HashSet<Uuid>,
    phantom_u: PhantomData<U>,
}

impl<O, S, T, U> WriteableSinkExecutor<O, S, T, U>
where
    O: 'static + WriteableSink<S, T, U>,
    S: WriteableState<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
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
            operator: Arc::new(Mutex::new(operator_fn())),
            state: Arc::new(state_fn()),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            read_stream: Some(read_stream),
            helper: Some(OperatorExecutorHelper::new(operator_id)),
            phantom_u: PhantomData,
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
            "Node {}: Running Operator {}",
            self.config.node_id,
            self.config.get_name(),
        );

        tokio::task::block_in_place(|| {
            self.operator
                .lock()
                .unwrap()
                .run(self.read_stream.as_mut().unwrap())
        });

        // Run the setup method.
        let setup_context = OneInOneOutSetupContext::new(self.read_stream.as_ref().unwrap().id());

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        let process_stream_fut = helper.process_stream(
            read_stream,
            &mut (*self),
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
                            slog::error!(
                                crate::get_terminal_logger(),
                                "Operator executor {}: error receiving notifications {:?}",
                                self.operator_id(),
                                e,
                            );
                            break;
                        }
                    }
                }
            };
        }

        tokio::task::block_in_place(|| self.operator.lock().unwrap().destroy());

        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl<O, S, T, U> OperatorExecutorT for WriteableSinkExecutor<O, S, T, U>
where
    O: WriteableSink<S, T, U>,
    S: WriteableState<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
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

impl<O, S, T, U> OneInMessageProcessorT<T> for WriteableSinkExecutor<O, S, T, U>
where
    O: 'static + WriteableSink<S, T, U>,
    S: WriteableState<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
{
    // Generates an OperatorEvent for a message callback.
    fn message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
        // We do an unchecked get_mut here since the ordering is supposed to be enforced by the
        // lattice.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let time = msg.timestamp().clone();
        let config = self.config.clone();

        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || {
                operator.lock().unwrap().on_data(
                    &mut WriteableSinkContext::new(time, config, &state),
                    msg.data().unwrap(),
                )
            },
            OperatorType::Writeable,
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        // We do an unchecked get_mut here since the ordering is supposed to be enforced by the
        // lattice.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let time = timestamp.clone();

        OperatorEvent::new(
            timestamp.clone(),
            true,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || {
                operator
                    .lock()
                    .unwrap()
                    .on_watermark(&mut WriteableSinkContext::new(time, config, &state))
            },
            OperatorType::Writeable,
        )
    }
}

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
            &mut (*self),
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
    fn message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
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
            OperatorType::ReadOnly,
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
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
            OperatorType::ReadOnly,
        )
    }
}
