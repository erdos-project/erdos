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
    stream::{Stream, StreamExt},
    sync::{mpsc, watch, Mutex},
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
        Data, EventMakerT, Message, ReadStream, State, Timestamp, WriteStream,
    },
    node::lattice::ExecutionLattice,
    node::operator_event::OperatorEvent,
    scheduler::channel_manager::ChannelManager,
};

#[derive(Clone, Debug, PartialEq)]
enum EventRunnerMessage {
    AddedEvents,
    DestroyOperator,
}

// TODO: can optimized.
// Maybe there should be a node-wide pool of threads that run events...
/// An `event_runner` invocation is in charge of executing callbacks associated with an event.
/// Upon receipt of an `AddedEvents` notification, it queries the lattice for events that are
/// ready to run, executes them, and notifies the lattice of their completion.
async fn event_runner(
    lattice: Arc<ExecutionLattice>,
    mut notifier_rx: watch::Receiver<EventRunnerMessage>,
) {
    // Wait for notification for events added.
    while let Some(control_msg) = notifier_rx.recv().await {
        while let Some((event, event_id)) = lattice.get_event().await {
            (event.callback)();
            lattice.mark_as_completed(event_id).await;
        }
        if EventRunnerMessage::DestroyOperator == control_msg {
            break;
        }
    }
}

// TODO: use indirection to make this private.
pub trait OperatorExecutorT: Send {
    // Returns a future for OperatorExecutor::execute().
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>>;
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
    lattice: Arc<ExecutionLattice>,
    event_runner_handles: Option<Vec<tokio::task::JoinHandle<()>>>,
}

impl OperatorExecutorHelper {
    fn new() -> Self {
        OperatorExecutorHelper {
            lattice: Arc::new(ExecutionLattice::new()),
            event_runner_handles: None,
        }
    }

    async fn synchronize(&self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;
    }

    fn setup(
        &mut self,
        num_event_runners: usize,
    ) -> tokio::sync::watch::Sender<EventRunnerMessage> {
        // Set up the event runner.
        let (notifier_tx, notifier_rx) = watch::channel(EventRunnerMessage::AddedEvents);
        self.event_runner_handles = Some(Vec::with_capacity(num_event_runners));
        for _ in 0..num_event_runners {
            let event_runner_fut =
                tokio::task::spawn(event_runner(Arc::clone(&self.lattice), notifier_rx.clone()));
            self.event_runner_handles
                .as_mut()
                .unwrap()
                .push(event_runner_fut);
        }
        notifier_tx
    }

    async fn process_messages<T>(
        &self,
        mut read_stream: ReadStream<T>,
        message_processor: &dyn OneInMessageProcessorT<T>,
        notifier_tx: &tokio::sync::watch::Sender<EventRunnerMessage>,
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
                .broadcast(EventRunnerMessage::AddedEvents)
                .unwrap();
        }
    }

    async fn process_two_streams<T, U>(
        &self,
        mut left_read_stream: ReadStream<T>,
        mut right_read_stream: ReadStream<U>,
        message_processor: &dyn TwoInMessageProcessorT<T, U>,
        notifier_tx: &tokio::sync::watch::Sender<EventRunnerMessage>,
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
                .broadcast(EventRunnerMessage::AddedEvents)
                .unwrap();
        }
    }

    async fn teardown(&mut self, notifier_tx: &tokio::sync::watch::Sender<EventRunnerMessage>) {
        // Wait for event runners to finish.
        notifier_tx
            .broadcast(EventRunnerMessage::DestroyOperator)
            .unwrap();

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
        Self {
            config,
            operator: operator_fn(),
            state: state_fn(),
            write_stream,
            helper: OperatorExecutorHelper::new(),
        }
    }

    pub async fn execute(&mut self) {
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
    }
}

impl<O, S, T> OperatorExecutorT for SourceExecutor<O, S, T>
where
    O: Source<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
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
        Self {
            config,
            operator: operator_fn(),
            state: Arc::new(Mutex::new(state_fn())),
            read_ids: vec![read_stream.id()].into_iter().collect(),
            read_stream: Some(read_stream),
            helper: OperatorExecutorHelper::new(),
        }
    }

    pub async fn execute(&mut self) {
        self.helper.synchronize().await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| self.operator.run(self.read_stream.as_mut().unwrap()));

        let notifier_tx = self.helper.setup(self.config.num_event_runners);

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        self.helper
            .process_messages(read_stream, &(*self), &notifier_tx)
            .await;

        self.helper.teardown(&notifier_tx).await;

        tokio::task::block_in_place(|| self.operator.destroy());
    }
}

impl<O, S, T> OperatorExecutorT for SinkExecutor<O, S, T>
where
    O: Sink<S, T>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
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
        Self {
            config,
            operator: operator_fn(),
            state: Arc::new(Mutex::new(state_fn())),
            read_ids: vec![read_stream.id()].into_iter().collect(),
            write_ids: vec![write_stream.id()].into_iter().collect(),
            read_stream: Some(read_stream),
            write_stream,
            helper: OperatorExecutorHelper::new(),
        }
    }

    pub async fn execute(&mut self) {
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

        let notifier_tx = self.helper.setup(self.config.num_event_runners);

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        self.helper
            .process_messages(read_stream, &(*self), &notifier_tx)
            .await;

        self.helper.teardown(&notifier_tx).await;
        tokio::task::block_in_place(|| self.operator.destroy());

        // Close the stream.
        // TODO: check that the top watermark hasn't already been sent.
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::top()))
                .unwrap();
        }
    }
}

impl<O, S, T, U> OperatorExecutorT for OneInOneOutExecutor<O, S, T, U>
where
    O: OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
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
            helper: OperatorExecutorHelper::new(),
        }
    }

    pub async fn execute(&mut self) {
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

        let notifier_tx = self.helper.setup(self.config.num_event_runners);

        let left_read_stream: ReadStream<T> = self.left_read_stream.take().unwrap();
        let right_read_stream: ReadStream<U> = self.right_read_stream.take().unwrap();
        self.helper.process_two_streams(left_read_stream, right_read_stream, &(*self), &notifier_tx).await;

        self.helper.teardown(&notifier_tx).await;
        tokio::task::block_in_place(|| self.operator.destroy());

        // Close the stream.
        // TODO: check that the top watermark hasn't already been sent.
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::top()))
                .unwrap();
        }
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
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
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
            helper: OperatorExecutorHelper::new(),
        }
    }

    pub async fn execute(&mut self) {
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

        let notifier_tx = self.helper.setup(self.config.num_event_runners);

        let read_stream: ReadStream<T> = self.read_stream.take().unwrap();
        self.helper
            .process_messages(read_stream, &(*self), &notifier_tx)
            .await;

        self.helper.teardown(&notifier_tx).await;
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
    fn execute<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>> {
        Box::pin(self.execute())
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
