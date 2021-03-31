use std::{
    cell::RefCell,
    cmp,
    collections::{HashMap, HashSet},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{future, select};
use serde::Deserialize;
use tokio::{
    self,
    sync::{broadcast, mpsc, Mutex},
};

use crate::{
    communication::{ControlMessage, RecvEndpoint},
    dataflow::{
        operator::{
            OneInOneOut, OneInOneOutContext, OneInTwoOut, OneInTwoOutContext, OperatorConfig, Sink,
            SinkContext, Source, StatefulOneInOneOutContext, StatefulOneInTwoOutContext,
            StatefulSinkContext, StatefulTwoInOneOutContext, TwoInOneOut, TwoInOneOutContext,
        },
        stream::StreamId,
        stream::WriteStreamT,
        Data, Message, ReadStream, State, StreamT, Timestamp, WriteStream,
    },
    node::lattice::ExecutionLattice,
    node::operator_event::OperatorEvent,
    node::worker::EventNotification,
    scheduler::channel_manager::ChannelManager,
    OperatorId,
};

use super::worker::{OperatorExecutorNotification, WorkerNotification};

pub(crate) trait OperatorExecutorT: Send {
    /// Returns a future for OperatorExecutor::execute().
    fn execute<'a>(
        &'a mut self,
        channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>>;

    /// Returns the lattice into which the executor inserts events.
    fn lattice(&self) -> Arc<ExecutionLattice>;

    /// Returns the operator ID.
    fn operator_id(&self) -> OperatorId;
}

pub trait OneInMessageProcessorT<T>: Send + Sync
where
    T: Data + for<'a> Deserialize<'a>,
{
    // Generates an OperatorEvent for a stateless callback.
    fn stateless_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent;

    // Generates an OperatorEvent for a stateful callback.
    fn stateful_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent;

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&self, timestamp: &Timestamp) -> OperatorEvent;
}

pub trait TwoInMessageProcessorT<T, U>: Send + Sync
where
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    // Generates an OperatorEvent for a stateless callback on the first stream.
    fn left_stateless_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent;

    // Generates an OperatorEvent for a stateful callback on the first stream.
    fn left_stateful_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent;

    // Generates an OperatorEvent for a stateless callback on the second stream.
    fn right_stateless_cb_event(&self, msg: Arc<Message<U>>) -> OperatorEvent;

    // Generates an OperatorEvent for a stateful callback on the second stream.
    fn right_stateful_cb_event(&self, msg: Arc<Message<U>>) -> OperatorEvent;

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&self, timestamp: &Timestamp) -> OperatorEvent;
}

pub struct OperatorExecutorHelper {
    operator_id: OperatorId,
    lattice: Arc<ExecutionLattice>,
    event_runner_handles: Option<Vec<tokio::task::JoinHandle<()>>>,
}

impl OperatorExecutorHelper {
    fn new(operator_id: OperatorId) -> Self {
        OperatorExecutorHelper {
            operator_id,
            lattice: Arc::new(ExecutionLattice::new()),
            event_runner_handles: None,
        }
    }

    async fn synchronize(&self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;
    }

    async fn process_stream<T>(
        &self,
        mut read_stream: ReadStream<T>,
        message_processor: &dyn OneInMessageProcessorT<T>,
        notifier_tx: &tokio::sync::broadcast::Sender<EventNotification>,
    ) where
        T: Data + for<'a> Deserialize<'a>,
    {
        while let Ok(msg) = read_stream.async_read().await {
            // TODO: optimize so that stateful callbacks not invoked if not needed.
            let events = match msg.data() {
                // Data message
                Some(_) => {
                    // Stateless callback.
                    let msg_ref = Arc::clone(&msg);
                    let stateless_data_event = message_processor.stateless_cb_event(msg_ref);

                    // Stateful callback
                    let msg_ref = Arc::clone(&msg);
                    let stateful_data_event = message_processor.stateful_cb_event(msg_ref);
                    vec![stateless_data_event, stateful_data_event]
                }
                // Watermark
                None => {
                    let watermark_event = message_processor.watermark_cb_event(msg.timestamp());
                    vec![watermark_event]
                }
            };

            self.lattice.add_events(events).await;
            notifier_tx
                .send(EventNotification::AddedEvents(self.operator_id))
                .unwrap();
        }
    }

    async fn process_two_streams<T, U>(
        &self,
        mut left_read_stream: ReadStream<T>,
        mut right_read_stream: ReadStream<U>,
        message_processor: &dyn TwoInMessageProcessorT<T, U>,
        notifier_tx: &tokio::sync::broadcast::Sender<EventNotification>,
    ) where
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
    {
        let mut left_watermark = Timestamp::bottom();
        let mut right_watermark = Timestamp::bottom();
        let mut min_watermark = cmp::min(&left_watermark, &right_watermark).clone();
        loop {
            let events = tokio::select! {
                Ok(left_msg) = left_read_stream.async_read() => {
                    match left_msg.data() {
                        // Data message
                        Some(_) => {
                            // Stateless callback.
                            let msg_ref = Arc::clone(&left_msg);
                            let data_event = message_processor.left_stateless_cb_event(msg_ref);

                            // Stateful callback
                            let msg_ref = Arc::clone(&left_msg);
                            let stateful_data_event = message_processor.left_stateful_cb_event(msg_ref);
                            vec![data_event, stateful_data_event]
                        }
                        // Watermark
                        None => {
                            left_watermark = left_msg.timestamp().clone();
                            let advance_watermark = cmp::min(&left_watermark, &right_watermark) > &min_watermark;
                            if advance_watermark {
                                min_watermark = left_watermark.clone();
                                vec![message_processor.watermark_cb_event(&left_msg.timestamp().clone())]
                            } else {
                                Vec::new()
                            }
                        }
                    }
                }
                Ok(right_msg) = right_read_stream.async_read() => {
                    match right_msg.data() {
                        // Data message
                        Some(_) => {
                            // Stateless callback.
                            let msg_ref = Arc::clone(&right_msg);
                            let data_event = message_processor.right_stateless_cb_event(msg_ref);

                            // Stateful callback
                            let msg_ref = Arc::clone(&right_msg);
                            let stateful_data_event = message_processor.right_stateful_cb_event(msg_ref);
                            vec![data_event, stateful_data_event]
                        }
                        // Watermark
                        None => {
                            right_watermark = right_msg.timestamp().clone();
                            let advance_watermark = cmp::min(&left_watermark, &right_watermark) > &min_watermark;
                            if advance_watermark {
                                min_watermark = right_watermark.clone();
                                vec![message_processor.watermark_cb_event(&right_msg.timestamp().clone())]
                            } else {
                                Vec::new()
                            }
                        }
                    }
                }
                else => break,
            };
            self.lattice.add_events(events).await;
            notifier_tx
                .send(EventNotification::AddedEvents(self.operator_id))
                .unwrap();
        }
    }

    async fn teardown(&mut self, notifier_tx: &tokio::sync::broadcast::Sender<EventNotification>) {
        // Wait for event runners to finish.
        // notifier_tx
        //     .send(EventNotification::DestroyOperator(self.operator_id))
        //     .unwrap();

        // Handle errors?
        let event_runner_handles = self.event_runner_handles.take().unwrap();
        future::join_all(event_runner_handles).await;
    }
}

pub struct SourceExecutor<O, S, T>
where
    O: Source<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: O,
    state: S,
    write_stream: WriteStream<T>,
    helper: OperatorExecutorHelper,
}

impl<O, S, T> SourceExecutor<O, S, T>
where
    O: Source<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        write_stream: WriteStream<T>,
    ) -> Self {
        let operator_id = config.id;
        Self {
            config,
            operator: operator_fn(),
            state: state_fn(),
            write_stream,
            helper: OperatorExecutorHelper::new(operator_id),
        }
    }

    pub(crate) async fn execute(
        &mut self,
        _channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        _channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        self.helper.synchronize().await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| self.operator.run(&mut self.write_stream));
        tokio::task::block_in_place(|| self.operator.destroy());

        // Close the stream.
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::top()))
                .unwrap();
        }

        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl<O, S, T> OperatorExecutorT for SourceExecutor<O, S, T>
where
    O: Source<S, T>,
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
        Arc::clone(&self.helper.lattice)
    }

    fn operator_id(&self) -> OperatorId {
        self.config.id
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
    helper: OperatorExecutorHelper,
    read_ids: HashSet<StreamId>,
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
            read_ids: vec![read_stream.id()].into_iter().collect(),
            read_stream: Some(read_stream),
            helper: OperatorExecutorHelper::new(operator_id),
        }
    }

    pub(crate) async fn execute(
        &mut self,
        mut channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        self.helper.synchronize().await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| self.operator.run(self.read_stream.as_mut().unwrap()));

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        let process_stream_fut =
            self.helper
                .process_stream(read_stream, &(*self), &channel_to_event_runners);
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

        self.helper.teardown(&channel_to_event_runners).await;

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
        Arc::clone(&self.helper.lattice)
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
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            self.read_ids.clone(),
            HashSet::new(),
            move || O::on_data_stateful(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&self, timestamp: &Timestamp) -> OperatorEvent {
        let mut ctx = StatefulSinkContext {
            timestamp: timestamp.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            timestamp.clone(),
            true,
            0,
            self.read_ids.clone(),
            HashSet::new(),
            move || O::on_watermark(&mut ctx),
        )
    }
}

pub struct OneInOneOutExecutor<O, S, T, U>
where
    O: OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: O,
    state: Arc<Mutex<S>>,
    read_stream: Option<ReadStream<T>>,
    write_stream: WriteStream<U>,
    helper: OperatorExecutorHelper,
    read_ids: HashSet<StreamId>,
    write_ids: HashSet<StreamId>,
}

impl<O, S, T, U> OneInOneOutExecutor<O, S, T, U>
where
    O: OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        read_stream: ReadStream<T>,
        write_stream: WriteStream<U>,
    ) -> Self {
        let operator_id = config.id;
        Self {
            config,
            operator: operator_fn(),
            state: Arc::new(Mutex::new(state_fn())),
            read_ids: vec![read_stream.id()].into_iter().collect(),
            write_ids: vec![write_stream.id()].into_iter().collect(),
            read_stream: Some(read_stream),
            write_stream,
            helper: OperatorExecutorHelper::new(operator_id),
        }
    }

    pub(crate) async fn execute(
        &mut self,
        mut channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        self.helper.synchronize().await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| {
            self.operator
                .run(self.read_stream.as_mut().unwrap(), &mut self.write_stream)
        });

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        let process_stream_fut =
            self.helper
                .process_stream(read_stream, &(*self), &channel_to_event_runners);
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

        self.helper.teardown(&channel_to_event_runners).await;
        tokio::task::block_in_place(|| self.operator.destroy());

        // Close the stream.
        // TODO: check that the top watermark hasn't already been sent.
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::top()))
                .unwrap();
        }

        channel_to_worker
            .send(WorkerNotification::DestroyedOperator(self.operator_id()))
            .unwrap();
    }
}

impl<O, S, T, U> OperatorExecutorT for OneInOneOutExecutor<O, S, T, U>
where
    O: OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
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
        Arc::clone(&self.helper.lattice)
    }

    fn operator_id(&self) -> OperatorId {
        self.config.id
    }
}

impl<O, S, T, U> OneInMessageProcessorT<T> for OneInOneOutExecutor<O, S, T, U>
where
    O: OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn stateless_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = OneInOneOutContext {
            timestamp: msg.timestamp().clone(),
            write_stream: self.write_stream.clone(),
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
        let mut ctx = StatefulOneInOneOutContext {
            timestamp: msg.timestamp().clone(),
            write_stream: self.write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            self.read_ids.clone(),
            self.write_ids.clone(),
            move || O::on_data_stateful(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&self, timestamp: &Timestamp) -> OperatorEvent {
        let mut ctx = StatefulOneInOneOutContext {
            timestamp: timestamp.clone(),
            write_stream: self.write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        if self.config.flow_watermarks {
            let mut write_stream_copy = self.write_stream.clone();
            let timestamp_copy = timestamp.clone();
            OperatorEvent::new(
                timestamp.clone(),
                true,
                0,
                self.read_ids.clone(),
                self.write_ids.clone(),
                move || {
                    O::on_watermark(&mut ctx);
                    write_stream_copy
                        .send(Message::new_watermark(timestamp_copy))
                        .ok();
                },
            )
        } else {
            OperatorEvent::new(
                timestamp.clone(),
                true,
                0,
                self.read_ids.clone(),
                self.write_ids.clone(),
                move || {
                    O::on_watermark(&mut ctx);
                },
            )
        }
    }
}

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
    read_ids: HashSet<StreamId>,
    write_ids: HashSet<StreamId>,
    helper: OperatorExecutorHelper,
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
            read_ids: vec![left_read_stream.id(), right_read_stream.id()]
                .into_iter()
                .collect(),
            left_read_stream: Some(left_read_stream),
            right_read_stream: Some(right_read_stream),
            write_ids: vec![write_stream.id()].into_iter().collect(),
            write_stream,
            helper: OperatorExecutorHelper::new(operator_id),
        }
    }

    pub(crate) async fn execute(
        &mut self,
        mut channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        self.helper.synchronize().await;

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
        let process_stream_fut = self.helper.process_two_streams(
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

        self.helper.teardown(&channel_to_event_runners).await;
        tokio::task::block_in_place(|| self.operator.destroy());

        // Close the stream.
        // TODO: check that the top watermark hasn't already been sent.
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::top()))
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
        Arc::clone(&self.helper.lattice)
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
            write_stream: self.write_stream.clone(),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || O::on_left_data(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a stateful callback on the first stream.
    fn left_stateful_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = StatefulTwoInOneOutContext {
            timestamp: msg.timestamp().clone(),
            write_stream: self.write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            self.read_ids.clone(),
            self.write_ids.clone(),
            move || O::on_left_data_stateful(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a stateless callback on the second stream.
    fn right_stateless_cb_event(&self, msg: Arc<Message<U>>) -> OperatorEvent {
        let mut ctx = TwoInOneOutContext {
            timestamp: msg.timestamp().clone(),
            write_stream: self.write_stream.clone(),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            move || O::on_right_data(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a stateful callback on the second stream.
    fn right_stateful_cb_event(&self, msg: Arc<Message<U>>) -> OperatorEvent {
        let mut ctx = StatefulTwoInOneOutContext {
            timestamp: msg.timestamp().clone(),
            write_stream: self.write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            self.read_ids.clone(),
            self.write_ids.clone(),
            move || O::on_right_data_stateful(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&self, timestamp: &Timestamp) -> OperatorEvent {
        let mut ctx = StatefulTwoInOneOutContext {
            timestamp: timestamp.clone(),
            write_stream: self.write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        if self.config.flow_watermarks {
            let timestamp_copy = timestamp.clone();
            let mut write_stream_copy = self.write_stream.clone();
            OperatorEvent::new(
                timestamp.clone(),
                true,
                0,
                self.read_ids.clone(),
                self.write_ids.clone(),
                move || {
                    O::on_watermark(&mut ctx);
                    write_stream_copy
                        .send(Message::new_watermark(timestamp_copy))
                        .ok();
                },
            )
        } else {
            OperatorEvent::new(
                timestamp.clone(),
                true,
                0,
                self.read_ids.clone(),
                self.write_ids.clone(),
                move || O::on_watermark(&mut ctx),
            )
        }
    }
}

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
    helper: OperatorExecutorHelper,
    read_ids: HashSet<StreamId>,
    write_ids: HashSet<StreamId>,
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
            read_ids: vec![read_stream.id()].into_iter().collect(),
            write_ids: vec![left_write_stream.id(), right_write_stream.id()]
                .into_iter()
                .collect(),
            read_stream: Some(read_stream),
            left_write_stream,
            right_write_stream,
            helper: OperatorExecutorHelper::new(operator_id),
        }
    }

    pub(crate) async fn execute(
        &mut self,
        mut channel_from_worker: broadcast::Receiver<OperatorExecutorNotification>,
        channel_to_worker: mpsc::UnboundedSender<WorkerNotification>,
        channel_to_event_runners: broadcast::Sender<EventNotification>,
    ) {
        self.helper.synchronize().await;

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

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        let process_stream_fut =
            self.helper
                .process_stream(read_stream, &(*self), &channel_to_event_runners);
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

        self.helper.teardown(&channel_to_event_runners).await;
        tokio::task::block_in_place(|| self.operator.destroy());

        // Close the stream.
        // TODO: check that the top watermark hasn't already been sent.
        if !self.left_write_stream.is_closed() {
            self.left_write_stream
                .send(Message::new_watermark(Timestamp::top()))
                .unwrap();
        }
        if !self.right_write_stream.is_closed() {
            self.right_write_stream
                .send(Message::new_watermark(Timestamp::top()))
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
        Arc::clone(&self.helper.lattice)
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
    fn stateless_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = OneInTwoOutContext {
            timestamp: msg.timestamp().clone(),
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
        )
    }

    // Generates an OperatorEvent for a stateful callback.
    fn stateful_cb_event(&self, msg: Arc<Message<T>>) -> OperatorEvent {
        let mut ctx = StatefulOneInTwoOutContext {
            timestamp: msg.timestamp().clone(),
            left_write_stream: self.left_write_stream.clone(),
            right_write_stream: self.right_write_stream.clone(),
            state: Arc::clone(&self.state),
        };
        OperatorEvent::new(
            msg.timestamp().clone(),
            false,
            0,
            self.read_ids.clone(),
            self.write_ids.clone(),
            move || O::on_data_stateful(&mut ctx, msg.data().unwrap()),
        )
    }

    // Generates an OperatorEvent for a watermark callback.
    fn watermark_cb_event(&self, timestamp: &Timestamp) -> OperatorEvent {
        let mut ctx = StatefulOneInTwoOutContext {
            timestamp: timestamp.clone(),
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
                0,
                self.read_ids.clone(),
                self.write_ids.clone(),
                move || {
                    O::on_watermark(&mut ctx);
                    left_write_stream_copy
                        .send(Message::new_watermark(timestamp_copy_left))
                        .ok();
                    right_write_stream_copy
                        .send(Message::new_watermark(timestamp_copy_right))
                        .ok();
                },
            )
        } else {
            OperatorEvent::new(
                timestamp.clone(),
                true,
                0,
                self.read_ids.clone(),
                self.write_ids.clone(),
                move || {
                    O::on_watermark(&mut ctx);
                },
            )
        }
    }
}
