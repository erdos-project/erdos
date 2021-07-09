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
            OneInOneOut, OneInOneOutContext, OneInOneOutSetupContext, OperatorConfig,
            ParallelOneInOneOut, ParallelOneInOneOutContext,
        },
        stream::WriteStreamT,
        Data, Message, ReadOnlyState, ReadStream, StreamT, Timestamp, WriteStream, WriteableState,
    },
    node::{
        lattice::ExecutionLattice,
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::{OneInMessageProcessorT, OperatorExecutorHelper, OperatorExecutorT},
        worker::{EventNotification, OperatorExecutorNotification, WorkerNotification},
    },
    OperatorId, Uuid,
};

pub struct OneInOneOutExecutor<T>
where
    T: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    executor: Box<dyn OneInMessageProcessorT<T>>,
    helper: Option<OperatorExecutorHelper>,
    read_stream: Option<ReadStream<T>>,
}

impl<T> OneInOneOutExecutor<T>
where
    T: Data + for<'a> Deserialize<'a>,
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
        // TODO (Sukrit): Implement deadlines and `setup` method for the new OneInOneOut operators.

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

        // Shutdown.
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
                                "OneInOneOut Executor {}: error receiving notifications {:?}",
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

        // Ask the executor to cleanup and notify the worker.
        self.executor.cleanup();
        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl<T> OperatorExecutorT for OneInOneOutExecutor<T>
where
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

pub struct ParallelOneInOneOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelOneInOneOut<S, T, U, V, W>,
    S: ReadOnlyState<V, W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: 'static + Send + Sync,
    W: 'static + Send + Sync,
{
    config: OperatorConfig,
    operator: Arc<O>,
    state: Arc<S>,
    state_ids: HashSet<Uuid>,
    write_stream: WriteStream<U>,
    phantom_t: PhantomData<T>,
    phantom_v: PhantomData<V>,
    phantom_w: PhantomData<W>,
}

impl<O, S, T, U, V, W> ParallelOneInOneOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelOneInOneOut<S, T, U, V, W>,
    S: ReadOnlyState<V, W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: 'static + Send + Sync,
    W: 'static + Send + Sync,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        write_stream: WriteStream<U>,
    ) -> Self {
        Self {
            config,
            operator: Arc::new(operator_fn()),
            state: Arc::new(state_fn()),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            write_stream,
            phantom_t: PhantomData,
            phantom_v: PhantomData,
            phantom_w: PhantomData,
        }
    }
}

impl<O, S, T, U, V, W> OneInMessageProcessorT<T>
    for ParallelOneInOneOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelOneInOneOut<S, T, U, V, W>,
    S: ReadOnlyState<V, W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: 'static + Send + Sync,
    W: 'static + Send + Sync,
{
    fn execute_run(&mut self, read_stream: &mut ReadStream<T>) {
        Arc::get_mut(&mut self.operator)
            .unwrap()
            .run(read_stream, &mut self.write_stream);
    }

    fn execute_destroy(&mut self) {
        Arc::get_mut(&mut self.operator).unwrap().destroy();
    }

    fn cleanup(&mut self) {
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .expect(&format!(
                    "[ParallelOneInOneOut] Error sending Top watermark for operator {}",
                    self.config.get_name()
                ));
        }
    }

    fn message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
        // Clone the reference to the operator and the state.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let time = msg.timestamp().clone();
        let config = self.config.clone();
        let write_stream = self.write_stream.clone();

        OperatorEvent::new(
            time.clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || {
                operator.on_data(
                    &ParallelOneInOneOutContext::new(time, config, &state, write_stream),
                    msg.data().unwrap(),
                )
            },
            OperatorType::Parallel,
        )
    }

    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        // Clone the reference to the operator and the state.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let time = timestamp.clone();
        let config = self.config.clone();
        let write_stream = self.write_stream.clone();

        if self.config.flow_watermarks {
            let mut write_stream_copy = self.write_stream.clone();
            let time_copy = time.clone();
            OperatorEvent::new(
                time.clone(),
                true,
                127,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    operator.on_watermark(&mut ParallelOneInOneOutContext::new(
                        time,
                        config,
                        &state,
                        write_stream,
                    ));
                    write_stream_copy
                        .send(Message::new_watermark(time_copy))
                        .ok();
                },
                OperatorType::Parallel,
            )
        } else {
            OperatorEvent::new(
                time.clone(),
                true,
                0,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    operator.on_watermark(&mut ParallelOneInOneOutContext::new(
                        time,
                        config,
                        &state,
                        write_stream,
                    ))
                },
                OperatorType::Parallel,
            )
        }
    }
}

pub struct OneInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + OneInOneOut<S, T, U, V>,
    S: WriteableState<V>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: Arc<Mutex<O>>,
    state: Arc<Mutex<S>>,
    state_ids: HashSet<Uuid>,
    write_stream: WriteStream<U>,
    phantom_t: PhantomData<T>,
    phantom_v: PhantomData<V>,
}

impl<O, S, T, U, V> OneInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + OneInOneOut<S, T, U, V>,
    S: WriteableState<V>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        write_stream: WriteStream<U>,
    ) -> Self {
        Self {
            config,
            operator: Arc::new(Mutex::new(operator_fn())),
            state: Arc::new(Mutex::new(state_fn())),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            write_stream,
            phantom_t: PhantomData,
            phantom_v: PhantomData,
        }
    }
}

impl<O, S, T, U, V> OneInMessageProcessorT<T> for OneInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + OneInOneOut<S, T, U, V>,
    S: WriteableState<V>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: 'static + Send + Sync,
{
    fn execute_run(&mut self, read_stream: &mut ReadStream<T>) {
        self.operator
            .lock()
            .unwrap()
            .run(read_stream, &mut self.write_stream);
    }

    fn execute_destroy(&mut self) {
        self.operator.lock().unwrap().destroy();
    }

    fn cleanup(&mut self) {
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .expect(&format!(
                    "[OneInOneOut] Error sending Top watermark for operator {}",
                    self.config.get_name()
                ));
        }
    }

    fn message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
        // Clone the reference to the operator and the state.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let time = msg.timestamp().clone();
        let config = self.config.clone();
        let write_stream = self.write_stream.clone();

        OperatorEvent::new(
            time.clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || {
                operator.lock().unwrap().on_data(
                    &mut OneInOneOutContext::new(
                        time,
                        config,
                        &mut state.lock().unwrap(),
                        write_stream,
                    ),
                    msg.data().unwrap(),
                )
            },
            OperatorType::Sequential,
        )
    }

    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        // Clone the reference to the operator and the state.
        let operator = Arc::clone(&self.operator);
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let time = timestamp.clone();
        let write_stream = self.write_stream.clone();

        if self.config.flow_watermarks {
            let mut write_stream_copy = self.write_stream.clone();
            let time_copy = time.clone();
            OperatorEvent::new(
                time.clone(),
                true,
                127,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    operator
                        .lock()
                        .unwrap()
                        .on_watermark(&mut OneInOneOutContext::new(
                            time,
                            config,
                            &mut state.lock().unwrap(),
                            write_stream,
                        ));
                    write_stream_copy
                        .send(Message::new_watermark(time_copy))
                        .ok();
                },
                OperatorType::Sequential,
            )
        } else {
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
                        .on_watermark(&mut OneInOneOutContext::new(
                            time,
                            config,
                            &mut state.lock().unwrap(),
                            write_stream,
                        ))
                },
                OperatorType::Sequential,
            )
        }
    }
}
