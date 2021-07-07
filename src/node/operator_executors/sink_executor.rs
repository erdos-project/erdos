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
            OneInOneOutSetupContext, OperatorConfig, ReadOnlySink, ReadOnlySinkContext,
            WriteableSink, WriteableSinkContext,
        },
        Data, Message, ReadOnlyState, ReadStream, StreamT, Timestamp, WriteableState,
    },
    node::{
        lattice::ExecutionLattice,
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::{OneInMessageProcessorT, OperatorExecutorHelper, OperatorExecutorT},
        worker::{EventNotification, OperatorExecutorNotification, WorkerNotification},
    },
    OperatorId, Uuid,
};

pub struct SinkExecutor<T>
where
    T: Data + for<'b> Deserialize<'b>,
{
    config: OperatorConfig,
    executor: Box<dyn OneInMessageProcessorT<T>>,
    helper: Option<OperatorExecutorHelper>,
    read_stream: Option<ReadStream<T>>,
}

impl<T> SinkExecutor<T>
where
    T: Data + for<'b> Deserialize<'b>,
{
    pub fn new(
        config: OperatorConfig,
        executor: Box<dyn OneInMessageProcessorT<T>>,
        read_stream: ReadStream<T>,
    ) -> Self {
        let operator_id = config.id;
        Self {
            config,
            executor,
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
        // Synchronize the operator with the rest of the dataflow graph.
        let mut helper = self.helper.take().unwrap();
        helper.synchronize().await;

        // Run the `setup` method.
        let mut read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        let setup_context = OneInOneOutSetupContext::new(read_stream.id());
        // TODO (Sukrit): Implement deadlines and `setup` method for the `Sink` operators.

        // Execute the `run` method.
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: Running Operator {}",
            self.config.node_id,
            self.config.get_name(),
        );

        tokio::task::block_in_place(|| {
            self.executor.execute_run(&mut read_stream);
        });

        // Process messages on the incoming stream.
        let process_stream_fut = helper.process_stream(
            read_stream,
            &mut (*self.executor),
            &channel_to_event_runners,
            &setup_context,
        );

        // Shutdown and cleanup.
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
                                "Sink Executor {}: error receiving notifications {:?}",
                                self.operator_id(),
                                e,
                                );
                            break;
                        }
                    }
                }
            }
        }

        // Invoke the `destroy` method.
        tokio::task::block_in_place(|| self.executor.execute_destroy());

        // Return the helper.
        self.helper.replace(helper);

        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl<T> OperatorExecutorT for SinkExecutor<T>
where
    T: Data + for<'b> Deserialize<'b>,
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

pub struct ReadOnlySinkMessageProcessor<O, S, T, U, V>
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
    state_ids: HashSet<Uuid>,
    phantom_t: PhantomData<T>,
    phantom_u: PhantomData<U>,
    phantom_v: PhantomData<V>,
}

impl<O, S, T, U, V> ReadOnlySinkMessageProcessor<O, S, T, U, V>
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
    ) -> Self {
        Self {
            config,
            operator: Arc::new(operator_fn()),
            state: Arc::new(state_fn()),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            phantom_t: PhantomData,
            phantom_u: PhantomData,
            phantom_v: PhantomData,
        }
    }
}

impl<O, S, T, U, V> OneInMessageProcessorT<T> for ReadOnlySinkMessageProcessor<O, S, T, U, V>
where
    O: 'static + ReadOnlySink<S, T, U, V>,
    S: ReadOnlyState<U, V>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    fn execute_run(&mut self, read_stream: &mut ReadStream<T>) {
        Arc::get_mut(&mut self.operator).unwrap().run(read_stream);
    }

    fn execute_destroy(&mut self) {
        Arc::get_mut(&mut self.operator).unwrap().destroy();
    }

    fn message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
        // Clone the reference to the operator and the state.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let time = msg.timestamp().clone();
        let config = self.config.clone();

        OperatorEvent::new(
            time.clone(),
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

    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        // Clone the reference to the operator and the state.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let time = timestamp.clone();
        let config = self.config.clone();

        OperatorEvent::new(
            time.clone(),
            true,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || operator.on_watermark(&mut ReadOnlySinkContext::new(time, config, &state)),
            OperatorType::ReadOnly,
        )
    }
}

pub struct WriteableSinkMessageProcessor<O, S, T, U>
where
    O: 'static + WriteableSink<S, T, U>,
    S: WriteableState<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
{
    config: OperatorConfig,
    operator: Arc<Mutex<O>>,
    state: Arc<Mutex<S>>,
    state_ids: HashSet<Uuid>,
    phantom_t: PhantomData<T>,
    phantom_u: PhantomData<U>,
}

impl<O, S, T, U> WriteableSinkMessageProcessor<O, S, T, U>
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
    ) -> Self {
        Self {
            config,
            operator: Arc::new(Mutex::new(operator_fn())),
            state: Arc::new(Mutex::new(state_fn())),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            phantom_t: PhantomData,
            phantom_u: PhantomData,
        }
    }
}

impl<O, S, T, U> OneInMessageProcessorT<T> for WriteableSinkMessageProcessor<O, S, T, U>
where
    O: 'static + WriteableSink<S, T, U>,
    S: WriteableState<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
{
    fn execute_run(&mut self, read_stream: &mut ReadStream<T>) {
        self.operator.lock().unwrap().run(read_stream);
    }

    fn execute_destroy(&mut self) {
        self.operator.lock().unwrap().destroy();
    }

    fn message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let time = msg.timestamp().clone();
        let config = self.config.clone();

        OperatorEvent::new(
            time.clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || {
                operator.lock().unwrap().on_data(
                    &mut WriteableSinkContext::new(time, config, &mut state.lock().unwrap()),
                    msg.data().unwrap(),
                )
            },
            OperatorType::Writeable,
        )
    }

    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let time = timestamp.clone();

        OperatorEvent::new(
            time.clone(),
            true,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || {
                operator
                    .lock()
                    .unwrap()
                    .on_watermark(&mut WriteableSinkContext::new(
                        time,
                        config,
                        &mut state.lock().unwrap(),
                    ))
            },
            OperatorType::Writeable,
        )
    }
}
