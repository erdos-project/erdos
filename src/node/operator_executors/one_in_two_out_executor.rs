use serde::Deserialize;
use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    dataflow::{
        operator::{
            OneInTwoOut, OneInTwoOutContext, OperatorConfig, ParallelOneInTwoOut,
            ParallelOneInTwoOutContext,
        },
        stream::WriteStreamT,
        AppendableStateT, Data, Message, ReadStream, StateT, Timestamp, WriteStream,
    },
    node::{
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::OneInMessageProcessorT,
    },
    Uuid,
};

pub struct ParallelOneInTwoOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelOneInTwoOut<S, T, U, V, W>,
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: Arc<O>,
    state: Arc<S>,
    state_ids: HashSet<Uuid>,
    left_write_stream: WriteStream<U>,
    right_write_stream: WriteStream<V>,
    phantom_t: PhantomData<T>,
    phantom_w: PhantomData<W>,
}

impl<O, S, T, U, V, W> ParallelOneInTwoOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelOneInTwoOut<S, T, U, V, W>,
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
    W: 'static + Send + Sync,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        left_write_stream: WriteStream<U>,
        right_write_stream: WriteStream<V>,
    ) -> Self {
        Self {
            config,
            operator: Arc::new(operator_fn()),
            state: Arc::new(state_fn()),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            left_write_stream,
            right_write_stream,
            phantom_t: PhantomData,
            phantom_w: PhantomData,
        }
    }
}

impl<O, S, T, U, V, W> OneInMessageProcessorT<T>
    for ParallelOneInTwoOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelOneInTwoOut<S, T, U, V, W>,
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
    W: 'static + Send + Sync,
{
    fn execute_run(&mut self, read_stream: &mut ReadStream<T>) {
        Arc::get_mut(&mut self.operator).unwrap().run(
            read_stream,
            &mut self.left_write_stream,
            &mut self.right_write_stream,
        );
    }

    fn execute_destroy(&mut self) {
        Arc::get_mut(&mut self.operator).unwrap().destroy();
    }

    fn cleanup(&mut self) {
        if !self.left_write_stream.is_closed() {
            self.left_write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .expect(&format!(
                    "[ParallelOneInTwoOut] Error sending Top watermark on left stream \
                    for operator {}",
                    self.config.get_name()
                ));
        }
        if !self.right_write_stream.is_closed() {
            self.right_write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .expect(&format!(
                    "[ParallelOneInTwoOut] Error sending Top watermark on right stream\
                    for operator {}",
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
        let left_write_stream = self.left_write_stream.clone();
        let right_write_stream = self.right_write_stream.clone();

        OperatorEvent::new(
            time.clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || {
                operator.on_data(
                    &ParallelOneInTwoOutContext::new(
                        time,
                        config,
                        &state,
                        left_write_stream,
                        right_write_stream,
                    ),
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
        let left_write_stream = self.left_write_stream.clone();
        let right_write_stream = self.right_write_stream.clone();

        if self.config.flow_watermarks {
            let mut left_write_stream_copy = self.left_write_stream.clone();
            let mut right_write_stream_copy = self.right_write_stream.clone();
            let time_copy_left = time.clone();
            let time_copy_right = time.clone();
            OperatorEvent::new(
                time.clone(),
                true,
                127,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    operator.on_watermark(&mut ParallelOneInTwoOutContext::new(
                        time,
                        config,
                        &state,
                        left_write_stream,
                        right_write_stream,
                    ));
                    left_write_stream_copy
                        .send(Message::new_watermark(time_copy_left))
                        .ok();
                    right_write_stream_copy
                        .send(Message::new_watermark(time_copy_right))
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
                    operator.on_watermark(&mut ParallelOneInTwoOutContext::new(
                        time,
                        config,
                        &state,
                        left_write_stream,
                        right_write_stream,
                    ))
                },
                OperatorType::Parallel,
            )
        }
    }
}

pub struct OneInTwoOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + OneInTwoOut<S, T, U, V>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: Arc<Mutex<O>>,
    state: Arc<Mutex<S>>,
    state_ids: HashSet<Uuid>,
    left_write_stream: WriteStream<U>,
    right_write_stream: WriteStream<V>,
    phantom_t: PhantomData<T>,
}

impl<O, S, T, U, V> OneInTwoOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + OneInTwoOut<S, T, U, V>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        left_write_stream: WriteStream<U>,
        right_write_stream: WriteStream<V>,
    ) -> Self {
        Self {
            config,
            operator: Arc::new(Mutex::new(operator_fn())),
            state: Arc::new(Mutex::new(state_fn())),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            left_write_stream,
            right_write_stream,
            phantom_t: PhantomData,
        }
    }
}

impl<O, S, T, U, V> OneInMessageProcessorT<T> for OneInTwoOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + OneInTwoOut<S, T, U, V>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn execute_run(&mut self, read_stream: &mut ReadStream<T>) {
        self.operator.lock().unwrap().run(
            read_stream,
            &mut self.left_write_stream,
            &mut self.right_write_stream,
        );
    }

    fn execute_destroy(&mut self) {
        self.operator.lock().unwrap().destroy();
    }

    fn cleanup(&mut self) {
        if !self.left_write_stream.is_closed() {
            self.left_write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .expect(&format!(
                    "[ParallelOneInTwoOut] Error sending Top watermark on left stream \
                    for operator {}",
                    self.config.get_name()
                ));
        }
        if !self.right_write_stream.is_closed() {
            self.right_write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .expect(&format!(
                    "[ParallelOneInTwoOut] Error sending Top watermark on right stream\
                    for operator {}",
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
        let left_write_stream = self.left_write_stream.clone();
        let right_write_stream = self.right_write_stream.clone();

        OperatorEvent::new(
            time.clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || {
                operator.lock().unwrap().on_data(
                    &mut OneInTwoOutContext::new(
                        time,
                        config,
                        &mut state.lock().unwrap(),
                        left_write_stream,
                        right_write_stream,
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
        let time = timestamp.clone();
        let config = self.config.clone();
        let left_write_stream = self.left_write_stream.clone();
        let right_write_stream = self.right_write_stream.clone();

        if self.config.flow_watermarks {
            let mut left_write_stream_copy = self.left_write_stream.clone();
            let mut right_write_stream_copy = self.right_write_stream.clone();
            let time_copy_left = time.clone();
            let time_copy_right = time.clone();
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
                        .on_watermark(&mut OneInTwoOutContext::new(
                            time,
                            config,
                            &mut state.lock().unwrap(),
                            left_write_stream,
                            right_write_stream,
                        ));
                    left_write_stream_copy
                        .send(Message::new_watermark(time_copy_left))
                        .ok();
                    right_write_stream_copy
                        .send(Message::new_watermark(time_copy_right))
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
                        .on_watermark(&mut OneInTwoOutContext::new(
                            time,
                            config,
                            &mut state.lock().unwrap(),
                            left_write_stream,
                            right_write_stream,
                        ))
                },
                OperatorType::Sequential,
            )
        }
    }
}

/*

pub struct OneInTwoOutExecutor<O, S, T, U, V>
where
    O: OneInTwoOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: O,
    state: Arc<Mutex<S>>,
    read_stream: Option<ReadStream<T>>,
    left_write_stream: WriteStream<U>,
    right_write_stream: WriteStream<V>,
    helper: Option<OperatorExecutorHelper>,
    state_ids: HashSet<Uuid>,
}

impl<O, S, T, U, V> OneInTwoOutExecutor<O, S, T, U, V>
where
    O: OneInTwoOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        read_stream: ReadStream<T>,
        left_write_stream: WriteStream<U>,
        right_write_stream: WriteStream<V>,
    ) -> Self {
        let operator_id = config.id;
        Self {
            config,
            operator: operator_fn(),
            state: Arc::new(Mutex::new(state_fn())),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            read_stream: Some(read_stream),
            left_write_stream,
            right_write_stream,
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

        tokio::task::block_in_place(|| {
            self.operator.run(
                self.read_stream.as_mut().unwrap(),
                &mut self.left_write_stream,
                &mut self.right_write_stream,
            )
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
        if !self.left_write_stream.is_closed() {
            self.left_write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .unwrap();
        }
        if !self.right_write_stream.is_closed() {
            self.right_write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .unwrap();
        }

        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl<O, S, T, U, V> OperatorExecutorT for OneInTwoOutExecutor<O, S, T, U, V>
where
    O: OneInTwoOut<S, T, U, V>,
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

impl<O, S, T, U, V> OneInMessageProcessorT<T> for OneInTwoOutExecutor<O, S, T, U, V>
where
    O: OneInTwoOut<S, T, U, V>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = OneInTwoOutContext {
            timestamp: msg.timestamp().clone(),
            config: self.config.clone(),
            left_write_stream: self.left_write_stream.clone(),
            right_write_stream: self.right_write_stream.clone(),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || O::on_data(&mut ctx, msg.data().unwrap()),
            OperatorType::Parallel,
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&mut self, timestamp: &Timestamp) -> OperatorEvent {
        let mut ctx = StatefulOneInTwoOutContext {
            timestamp: timestamp.clone(),
            config: self.config.clone(),
            left_write_stream: self.left_write_stream.clone(),
            right_write_stream: self.right_write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        if self.config.flow_watermarks {
            let mut left_write_stream_copy = self.left_write_stream.clone();
            let mut right_write_stream_copy = self.right_write_stream.clone();
            let timestamp_copy_left = timestamp.clone();
            let timestamp_copy_right = timestamp.clone();
            OperatorEvent::new(
                timestamp.clone(),
                true,
                127,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    O::on_watermark(&mut ctx);
                    left_write_stream_copy
                        .send(Message::new_watermark(timestamp_copy_left))
                        .ok();
                    right_write_stream_copy
                        .send(Message::new_watermark(timestamp_copy_right))
                        .ok();
                },
                OperatorType::Parallel,
            )
        } else {
            OperatorEvent::new(
                timestamp.clone(),
                true,
                0,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    O::on_watermark(&mut ctx);
                },
                OperatorType::Parallel,
            )
        }
    }
}*/
