use std::{
    cmp, collections::HashMap, collections::HashSet, future::Future, pin::Pin, sync::Arc,
    time::Duration,
};

use serde::Deserialize;
use tokio::{
    self,
    stream::StreamExt,
    sync::{broadcast, mpsc, Mutex},
    time::{delay_queue::Key, DelayQueue},
};

use crate::{
    dataflow::{
        deadlines::{ConditionContext, Deadline, HandlerContextT},
        operator::{
            OneInOneOut, OneInOneOutContext, OneInOneOutSetupContext, OneInTwoOut,
            OneInTwoOutContext, OperatorConfig, SetupContextT, Sink, SinkContext, Source,
            StatefulOneInOneOutContext, StatefulOneInTwoOutContext, StatefulSinkContext,
            StatefulTwoInOneOutContext, TwoInOneOut, TwoInOneOutContext,
        },
        stream::StreamId,
        stream::WriteStreamT,
        Data, Message, ReadStream, State, StreamT, Timestamp, WriteStream,
    },
    node::lattice::ExecutionLattice,
    node::operator_event::OperatorEvent,
    node::worker::EventNotification,
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
    condition_context: ConditionContext,
    deadline_queue: DelayQueue<(StreamId, Timestamp, Arc<dyn HandlerContextT>)>,
    stream_timestamp_to_key_map: HashMap<(StreamId, Timestamp), Key>,
}

impl OperatorExecutorHelper {
    fn new(operator_id: OperatorId) -> Self {
        OperatorExecutorHelper {
            operator_id,
            lattice: Arc::new(ExecutionLattice::new()),
            event_runner_handles: None,
            condition_context: ConditionContext::new(),
            deadline_queue: DelayQueue::new(),
            stream_timestamp_to_key_map: HashMap::new(),
        }
    }

    async fn synchronize(&self) {
        // TODO: replace this with a synchronization step
        // that ensures all operators are ready to run.
        tokio::time::delay_for(Duration::from_secs(1)).await;
    }

    fn manage_deadlines(
        &mut self,
        setup_context: &dyn SetupContextT,
        stream_id: StreamId,
        timestamp: Timestamp,
    ) {
        // Run the start and the end condition functions for all the deadlines.
        for deadline in setup_context.get_deadlines() {
            match deadline {
                Deadline::TimestampDeadline(d) => {
                    if d.start_condition(&self.condition_context) {
                        // Compute the deadline for the timestamp.
                        let deadline_duration = d.get_deadline(&self.condition_context);

                        // Install the handler into the queue with the given
                        // duration.
                        let queue_key: Key = self.deadline_queue.insert(
                            (stream_id, timestamp.clone(), d.get_handler()),
                            deadline_duration,
                        );

                        slog::debug!(
                            crate::TERMINAL_LOGGER,
                            "Installed a deadline handler with the Key: {:?} corresponding to \
                            Stream ID: {} and Timestamp: {:?}",
                            queue_key,
                            stream_id,
                            timestamp,
                        );

                        self.stream_timestamp_to_key_map
                            .insert((stream_id, timestamp.clone()), queue_key);
                    }

                    // Check the end condition.
                    if d.end_condition(&self.condition_context) {
                        // Remove the handler from the deadline queue.
                        match self
                            .stream_timestamp_to_key_map
                            .remove(&(stream_id, timestamp.clone()))
                        {
                            None => {
                                slog::warn!(
                                    crate::TERMINAL_LOGGER,
                                    "Could not find an installed deadline handler corresponding \
                                    to Stream ID: {} and Timestamp: {:?}",
                                    stream_id,
                                    timestamp,
                                );
                            }
                            Some(key) => {
                                // Remove the handler from the deadline queue.
                                slog::debug!(
                                    crate::TERMINAL_LOGGER,
                                    "Removing the deadline with the Key: {:?} corresponding to \
                                    Stream ID: {} and Timestamp: {:?} from the deadline queue.",
                                    key,
                                    stream_id,
                                    timestamp,
                                );
                                self.deadline_queue.remove(&key);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn process_stream<T>(
        &mut self,
        mut read_stream: ReadStream<T>,
        message_processor: &dyn OneInMessageProcessorT<T>,
        notifier_tx: &tokio::sync::broadcast::Sender<EventNotification>,
        setup_context: &dyn SetupContextT,
    ) where
        T: Data + for<'a> Deserialize<'a>,
    {
        loop {
            tokio::select! {
                // DelayQueue returns `None` if the queue is empty. This means that if there are no
                // deadlines installed, the queue will always be ready and return `None` thus
                // wasting resources. We can potentially fix this by inserting a Deadline for the
                // future and maintaining it so that the queue is not empty.
                Some(x) = self.deadline_queue.next() => {
                    // Missed a deadline. Invoke the handler.
                    // TODO (Sukrit): The handler is invoked in the thread of the OperatorExecutor.
                    // This may be an issue for long-running handlers since they block the
                    // processing of future messages. We can spawn these as a separate task.
                    let (stream_id, timestamp, handler_context) = x.unwrap().into_inner();
                    handler_context.invoke_handler(&self.condition_context);

                    // Remove the key from the hashmap.
                    match self.stream_timestamp_to_key_map.remove(&(stream_id, timestamp.clone())) {
                        None => {
                            slog::warn!(
                                crate::TERMINAL_LOGGER,
                                "Could not find a key corresponding to the Stream ID: {} \
                                and the Timestamp: {:?}",
                                stream_id,
                                timestamp,
                            );
                        }
                        Some(key) => {
                            slog::debug!(
                                crate::TERMINAL_LOGGER,
                                "Finished invoking the deadline handler for the Key: {:?} \
                                corresponding to the Stream ID: {} and the Timestamp: {:?}",
                                key, stream_id, timestamp);
                        }
                    }
                },
                // If there is a message on the ReadStream, then increment the messgae counts for
                // the given timestamp, evaluate the start and end condition and install / disarm
                // deadlines accordingly.
                // TODO (Sukrit) : The start and end conditions are evaluated in the thread of the
                // OperatorExecutor, and can be moved to a separate task if they become a
                // bottleneck.
                Ok(msg) = read_stream.async_read() => {
                    let events = match msg.data() {
                        // Data message
                        Some(_) => {
                            // Increment the message count and handle the arming and disarming of
                            // deadlines.
                            self.condition_context
                                .increment_msg_count(read_stream.id(), msg.timestamp().clone());
                            self.manage_deadlines(
                                setup_context,
                                read_stream.id(),
                                msg.timestamp().clone()
                            );


                            // TODO : Check if an event for both the stateful and the stateless
                            // callback is needed.

                            // Stateless callback.
                            let msg_ref = Arc::clone(&msg);
                            let stateless_data_event = message_processor.stateless_cb_event(msg_ref);

                            // Stateful callback
                            let msg_ref = Arc::clone(&msg);
                            let stateful_data_event = message_processor.stateful_cb_event(msg_ref);
                            vec![stateless_data_event, stateful_data_event]
                        },

                        // Watermark
                        None => {
                            // Set the watermark status and handle the arming and disarming of
                            // deadlines.
                            self.condition_context
                                .notify_watermark_arrival(read_stream.id(), msg.timestamp().clone());
                            self.manage_deadlines(
                                setup_context,
                                read_stream.id(),
                                msg.timestamp().clone()
                            );

                            let watermark_event = message_processor.watermark_cb_event(
                                msg.timestamp());
                            vec![watermark_event]
                        }
                    };

                    self.lattice.add_events(events).await;
                    notifier_tx
                        .send(EventNotification::AddedEvents(self.operator_id))
                        .unwrap();
                },
                else => break,
            }
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
        let mut left_watermark = Timestamp::Bottom;
        let mut right_watermark = Timestamp::Bottom;
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
                .send(Message::new_watermark(Timestamp::Top))
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
    helper: Option<OperatorExecutorHelper>,
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
        let mut setup_context: OneInOneOutSetupContext<usize> = OneInOneOutSetupContext::new();

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
    helper: Option<OperatorExecutorHelper>,
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

        // Run the setup method.
        let mut setup_context: OneInOneOutSetupContext<U> = OneInOneOutSetupContext::new();
        self.operator.setup(&mut setup_context);

        tokio::task::block_in_place(|| {
            self.operator
                .run(self.read_stream.as_mut().unwrap(), &mut self.write_stream)
        });

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
        Arc::clone(&self.helper.as_ref().unwrap().lattice)
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
            read_ids: vec![left_read_stream.id(), right_read_stream.id()]
                .into_iter()
                .collect(),
            left_read_stream: Some(left_read_stream),
            right_read_stream: Some(right_read_stream),
            write_ids: vec![write_stream.id()].into_iter().collect(),
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
    helper: Option<OperatorExecutorHelper>,
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
        let mut setup_context: OneInOneOutSetupContext<U> = OneInOneOutSetupContext::new();

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
