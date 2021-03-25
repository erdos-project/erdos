use std::{
    cell::RefCell,
    cmp,
    collections::{HashMap, HashSet},
    future::Future,
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
        Data, Message, ReadStream, State, StreamT, Timestamp, WriteStream,
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
        }
    }

    pub async fn execute(&mut self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;

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
    read_stream: ReadStream<T>,
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
            read_stream,
        }
    }

    pub async fn execute(&mut self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| self.operator.run(&mut self.read_stream));

        // Set up lattice and event runners.
        let lattice = Arc::new(ExecutionLattice::new());
        let (notifier_tx, notifier_rx) = watch::channel(EventRunnerMessage::AddedEvents);
        let mut event_runner_handles = Vec::new();
        for _ in 0..self.config.num_event_runners {
            let event_runner_fut =
                tokio::task::spawn(event_runner(Arc::clone(&lattice), notifier_rx.clone()));
            event_runner_handles.push(event_runner_fut);
        }

        // Get messages and insert events into lattice.
        let mut read_ids = HashSet::new();
        read_ids.insert(self.read_stream.id());
        while let Ok(msg) = self.read_stream.async_read().await {
            // TODO: optimize so that stateful callbacks not invoked if not needed.
            let events = match msg.data() {
                // Data message
                Some(_) => {
                    // Stateless callback.
                    let msg_ref = Arc::clone(&msg);
                    let mut ctx = SinkContext {
                        timestamp: msg.timestamp().clone(),
                    };
                    let data_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        false,
                        0,
                        HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                        HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                        move || O::on_data(&mut ctx, msg_ref.data().unwrap()),
                    );

                    // Stateful callback
                    let msg_ref = Arc::clone(&msg);
                    let mut ctx = StatefulSinkContext {
                        timestamp: msg.timestamp().clone(),
                        state: Arc::clone(&self.state),
                    };
                    let stateful_data_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        false,
                        0,
                        read_ids.clone(),
                        HashSet::new(),
                        move || O::on_data_stateful(&mut ctx, msg_ref.data().unwrap()),
                    );

                    vec![data_event, stateful_data_event]
                }
                // Watermark
                None => {
                    let mut ctx = StatefulSinkContext {
                        timestamp: msg.timestamp().clone(),
                        state: Arc::clone(&self.state),
                    };
                    let watermark_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        true,
                        0,
                        read_ids.clone(),
                        HashSet::new(),
                        move || O::on_watermark(&mut ctx),
                    );
                    vec![watermark_event]
                }
            };

            lattice.add_events(events).await;
            notifier_tx
                .broadcast(EventRunnerMessage::AddedEvents)
                .unwrap();
        }

        // Wait for event runners to finish.
        notifier_tx
            .broadcast(EventRunnerMessage::DestroyOperator)
            .unwrap();
        // Handle errors?
        future::join_all(event_runner_handles).await;

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
    read_stream: ReadStream<T>,
    write_stream: WriteStream<U>,
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
            read_stream,
            write_stream,
        }
    }

    pub async fn execute(&mut self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| {
            self.operator
                .run(&mut self.read_stream, &mut self.write_stream)
        });

        // Set up lattice and event runners.
        let lattice = Arc::new(ExecutionLattice::new());
        let (notifier_tx, notifier_rx) = watch::channel(EventRunnerMessage::AddedEvents);
        let mut event_runner_handles = Vec::new();
        for _ in 0..self.config.num_event_runners {
            let event_runner_fut =
                tokio::task::spawn(event_runner(Arc::clone(&lattice), notifier_rx.clone()));
            event_runner_handles.push(event_runner_fut);
        }

        // Get messages and insert events into lattice.
        let mut read_ids = HashSet::new();
        read_ids.insert(self.read_stream.id());
        let mut write_ids = HashSet::new();
        write_ids.insert(self.write_stream.id());
        while let Ok(msg) = self.read_stream.async_read().await {
            // TODO: optimize so that stateful callbacks not invoked if not needed.
            let events = match msg.data() {
                // Data message
                Some(_) => {
                    // Stateless callback.
                    let msg_ref = Arc::clone(&msg);
                    let mut ctx = OneInOneOutContext {
                        timestamp: msg.timestamp().clone(),
                        write_stream: self.write_stream.clone(),
                    };
                    let data_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        false,
                        0,
                        HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                        HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                        move || O::on_data(&mut ctx, msg_ref.data().unwrap()),
                    );

                    // Stateful callback
                    let msg_ref = Arc::clone(&msg);
                    let mut ctx = StatefulOneInOneOutContext {
                        timestamp: msg.timestamp().clone(),
                        write_stream: self.write_stream.clone(),
                        state: Arc::clone(&self.state),
                    };
                    let stateful_data_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        false,
                        0,
                        read_ids.clone(),
                        write_ids.clone(),
                        move || O::on_data_stateful(&mut ctx, msg_ref.data().unwrap()),
                    );

                    vec![data_event, stateful_data_event]
                }
                // Watermark
                None => {
                    let mut ctx = StatefulOneInOneOutContext {
                        timestamp: msg.timestamp().clone(),
                        write_stream: self.write_stream.clone(),
                        state: Arc::clone(&self.state),
                    };
                    let watermark_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        true,
                        0,
                        read_ids.clone(),
                        write_ids.clone(),
                        move || O::on_watermark(&mut ctx),
                    );

                    if self.config.flow_watermarks {
                        let timestamp = msg.timestamp().clone();
                        let mut write_stream_copy = self.write_stream.clone();
                        let flow_watermark_event = OperatorEvent::new(
                            msg.timestamp().clone(),
                            true,
                            127,
                            read_ids.clone(),
                            write_ids.clone(),
                            move || {
                                write_stream_copy
                                    .send(Message::new_watermark(timestamp))
                                    .ok();
                            },
                        );
                        vec![watermark_event, flow_watermark_event]
                    } else {
                        vec![watermark_event]
                    }
                }
            };

            lattice.add_events(events).await;
            notifier_tx
                .broadcast(EventRunnerMessage::AddedEvents)
                .unwrap();
        }

        // Wait for event runners to finish.
        notifier_tx
            .broadcast(EventRunnerMessage::DestroyOperator)
            .unwrap();
        // Handle errors?
        future::join_all(event_runner_handles).await;

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
    left_read_stream: ReadStream<T>,
    right_read_stream: ReadStream<U>,
    write_stream: WriteStream<V>,
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
            left_read_stream,
            right_read_stream,
            write_stream,
        }
    }

    pub async fn execute(&mut self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| {
            self.operator.run(
                &mut self.left_read_stream,
                &mut self.right_read_stream,
                &mut self.write_stream,
            )
        });

        // Set up lattice and event runners.
        let lattice = Arc::new(ExecutionLattice::new());
        let (notifier_tx, notifier_rx) = watch::channel(EventRunnerMessage::AddedEvents);
        let mut event_runner_handles = Vec::new();
        for _ in 0..self.config.num_event_runners {
            let event_runner_fut =
                tokio::task::spawn(event_runner(Arc::clone(&lattice), notifier_rx.clone()));
            event_runner_handles.push(event_runner_fut);
        }

        let mut read_ids = HashSet::new();
        read_ids.insert(self.left_read_stream.id());
        read_ids.insert(self.right_read_stream.id());

        let mut write_ids = HashSet::new();
        write_ids.insert(self.write_stream.id());

        let mut left_watermark = Timestamp::bottom();
        let mut right_watermark = Timestamp::bottom();
        let mut min_watermark = cmp::min(&left_watermark, &right_watermark).clone();

        loop {
            let events = tokio::select! {
                Ok(left_msg) = self.left_read_stream.async_read() => {
                    match left_msg.data() {
                        // Data message
                        Some(_) => {
                            // Stateless callback.
                            let msg_ref = Arc::clone(&left_msg);
                            let mut ctx = TwoInOneOutContext {
                                timestamp: left_msg.timestamp().clone(),
                                write_stream: self.write_stream.clone(),
                            };
                            let data_event = OperatorEvent::new(
                                left_msg.timestamp().clone(),
                                false,
                                0,
                                HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                                HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                                move || O::on_left_data(&mut ctx, msg_ref.data().unwrap()),
                            );

                            // Stateful callback
                            let msg_ref = Arc::clone(&left_msg);
                            let mut ctx = StatefulTwoInOneOutContext {
                                timestamp: left_msg.timestamp().clone(),
                                write_stream: self.write_stream.clone(),
                                state: Arc::clone(&self.state),
                            };
                            let stateful_data_event = OperatorEvent::new(
                                left_msg.timestamp().clone(),
                                false,
                                0,
                                read_ids.clone(),
                                write_ids.clone(),
                                move || O::on_left_data_stateful(&mut ctx, msg_ref.data().unwrap()),
                            );

                            vec![data_event, stateful_data_event]
                        }
                        // Watermark
                        None => {
                            left_watermark = left_msg.timestamp().clone();
                            let advance_watermark = cmp::min(&left_watermark, &right_watermark) > &min_watermark;
                            if advance_watermark {
                                min_watermark = left_watermark.clone();

                                let mut ctx = StatefulTwoInOneOutContext {
                                    timestamp: left_msg.timestamp().clone(),
                                    write_stream: self.write_stream.clone(),
                                    state: Arc::clone(&self.state),
                                };
                                let watermark_event = OperatorEvent::new(
                                    left_msg.timestamp().clone(),
                                    true,
                                    0,
                                    read_ids.clone(),
                                    write_ids.clone(),
                                    move || O::on_watermark(&mut ctx),
                                );

                                if self.config.flow_watermarks {
                                    let timestamp = left_msg.timestamp().clone();
                                    let mut write_stream_copy = self.write_stream.clone();
                                    let flow_watermark_event = OperatorEvent::new(
                                        left_msg.timestamp().clone(),
                                        true,
                                        127,
                                        read_ids.clone(),
                                        write_ids.clone(),
                                        move || {
                                            write_stream_copy
                                                .send(Message::new_watermark(timestamp))
                                                .ok();
                                        },
                                    );
                                    vec![watermark_event, flow_watermark_event]
                                } else {
                                    vec![watermark_event]
                                }
                            } else {
                                Vec::new()
                            }
                        }
                    }
                }
                Ok(right_msg) = self.right_read_stream.async_read() => {
                    match right_msg.data() {
                        // Data message
                        Some(_) => {
                            // Stateless callback.
                            let msg_ref = Arc::clone(&right_msg);
                            let mut ctx = TwoInOneOutContext {
                                timestamp: right_msg.timestamp().clone(),
                                write_stream: self.write_stream.clone(),
                            };
                            let data_event = OperatorEvent::new(
                                right_msg.timestamp().clone(),
                                false,
                                0,
                                HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                                HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                                move || O::on_right_data(&mut ctx, msg_ref.data().unwrap()),
                            );

                            // Stateful callback
                            let msg_ref = Arc::clone(&right_msg);
                            let mut ctx = StatefulTwoInOneOutContext {
                                timestamp: right_msg.timestamp().clone(),
                                write_stream: self.write_stream.clone(),
                                state: Arc::clone(&self.state),
                            };
                            let stateful_data_event = OperatorEvent::new(
                                right_msg.timestamp().clone(),
                                false,
                                0,
                                read_ids.clone(),
                                write_ids.clone(),
                                move || O::on_right_data_stateful(&mut ctx, msg_ref.data().unwrap()),
                            );

                            vec![data_event, stateful_data_event]
                        }
                        // Watermark
                        None => {
                            right_watermark = right_msg.timestamp().clone();
                            let advance_watermark = cmp::min(&right_watermark, &right_watermark) > &min_watermark;
                            if advance_watermark {
                                min_watermark = right_watermark.clone();

                                let mut ctx = StatefulTwoInOneOutContext {
                                    timestamp: right_msg.timestamp().clone(),
                                    write_stream: self.write_stream.clone(),
                                    state: Arc::clone(&self.state),
                                };
                                let watermark_event = OperatorEvent::new(
                                    right_msg.timestamp().clone(),
                                    true,
                                    0,
                                    read_ids.clone(),
                                    write_ids.clone(),
                                    move || O::on_watermark(&mut ctx),
                                );

                                if self.config.flow_watermarks {
                                    let timestamp = right_msg.timestamp().clone();
                                    let mut write_stream_copy = self.write_stream.clone();
                                    let flow_watermark_event = OperatorEvent::new(
                                        right_msg.timestamp().clone(),
                                        true,
                                        127,
                                        read_ids.clone(),
                                        write_ids.clone(),
                                        move || {
                                            write_stream_copy
                                                .send(Message::new_watermark(timestamp))
                                                .ok();
                                        },
                                    );
                                    vec![watermark_event, flow_watermark_event]
                                } else {
                                    vec![watermark_event]
                                }
                            } else {
                                Vec::new()
                            }
                        }
                    }
                }
                else => break,
            };
            lattice.add_events(events).await;
            notifier_tx
                .broadcast(EventRunnerMessage::AddedEvents)
                .unwrap();
        }

        // Wait for event runners to finish.
        notifier_tx
            .broadcast(EventRunnerMessage::DestroyOperator)
            .unwrap();
        // Handle errors?
        future::join_all(event_runner_handles).await;

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
    read_stream: ReadStream<T>,
    left_write_stream: WriteStream<U>,
    right_write_stream: WriteStream<V>,
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
            read_stream,
            left_write_stream,
            right_write_stream,
        }
    }

    pub async fn execute(&mut self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;

        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Node {}: running operator {}",
            self.config.node_id,
            self.config.get_name()
        );

        tokio::task::block_in_place(|| {
            self.operator.run(
                &mut self.read_stream,
                &mut self.left_write_stream,
                &mut self.right_write_stream,
            )
        });

        // Set up lattice and event runners.
        let lattice = Arc::new(ExecutionLattice::new());
        let (notifier_tx, notifier_rx) = watch::channel(EventRunnerMessage::AddedEvents);
        let mut event_runner_handles = Vec::new();
        for _ in 0..self.config.num_event_runners {
            let event_runner_fut =
                tokio::task::spawn(event_runner(Arc::clone(&lattice), notifier_rx.clone()));
            event_runner_handles.push(event_runner_fut);
        }

        // Get messages and insert events into lattice.
        let mut read_ids = HashSet::new();
        read_ids.insert(self.read_stream.id());
        let mut write_ids = HashSet::new();
        write_ids.insert(self.left_write_stream.id());
        write_ids.insert(self.right_write_stream.id());
        while let Ok(msg) = self.read_stream.async_read().await {
            // TODO: optimize so that stateful callbacks not invoked if not needed.
            let events = match msg.data() {
                // Data message
                Some(_) => {
                    // Stateless callback.
                    let msg_ref = Arc::clone(&msg);
                    let mut ctx = OneInTwoOutContext {
                        timestamp: msg.timestamp().clone(),
                        left_write_stream: self.left_write_stream.clone(),
                        right_write_stream: self.right_write_stream.clone(),
                    };
                    let data_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        false,
                        0,
                        HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                        HashSet::new(), // Used to manage state in lattice, should be cleaned up.
                        move || O::on_data(&mut ctx, msg_ref.data().unwrap()),
                    );

                    // Stateful callback
                    let msg_ref = Arc::clone(&msg);
                    let mut ctx = StatefulOneInTwoOutContext {
                        timestamp: msg.timestamp().clone(),
                        left_write_stream: self.left_write_stream.clone(),
                        right_write_stream: self.right_write_stream.clone(),
                        state: Arc::clone(&self.state),
                    };
                    let stateful_data_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        false,
                        0,
                        read_ids.clone(),
                        write_ids.clone(),
                        move || O::on_data_stateful(&mut ctx, msg_ref.data().unwrap()),
                    );

                    vec![data_event, stateful_data_event]
                }
                // Watermark
                None => {
                    let mut ctx = StatefulOneInTwoOutContext {
                        timestamp: msg.timestamp().clone(),
                        left_write_stream: self.left_write_stream.clone(),
                        right_write_stream: self.right_write_stream.clone(),
                        state: Arc::clone(&self.state),
                    };
                    let watermark_event = OperatorEvent::new(
                        msg.timestamp().clone(),
                        true,
                        0,
                        read_ids.clone(),
                        write_ids.clone(),
                        move || O::on_watermark(&mut ctx),
                    );

                    if self.config.flow_watermarks {
                        let timestamp = msg.timestamp().clone();
                        let mut left_write_stream_copy = self.left_write_stream.clone();
                        let mut right_write_stream_copy = self.right_write_stream.clone();
                        let flow_watermark_event = OperatorEvent::new(
                            msg.timestamp().clone(),
                            true,
                            127,
                            read_ids.clone(),
                            write_ids.clone(),
                            move || {
                                left_write_stream_copy
                                    .send(Message::new_watermark(timestamp.clone()))
                                    .ok();
                                right_write_stream_copy
                                    .send(Message::new_watermark(timestamp))
                                    .ok();
                            },
                        );
                        vec![watermark_event, flow_watermark_event]
                    } else {
                        vec![watermark_event]
                    }
                }
            };

            lattice.add_events(events).await;
            notifier_tx
                .broadcast(EventRunnerMessage::AddedEvents)
                .unwrap();
        }

        // Wait for event runners to finish.
        notifier_tx
            .broadcast(EventRunnerMessage::DestroyOperator)
            .unwrap();
        // Handle errors?
        future::join_all(event_runner_handles).await;

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
