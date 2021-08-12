use serde::Deserialize;
use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    dataflow::{
        context::{OneInTwoOutContext, ParallelOneInTwoOutContext, SetupContext},
        deadlines::{ConditionContext, DeadlineEvent, DeadlineId},
        operator::{OneInTwoOut, OperatorConfig, ParallelOneInTwoOut},
        stream::{StreamId, StreamT, WriteStreamT},
        AppendableStateT, Data, Message, ReadStream, StateT, Timestamp, WriteStream,
    },
    node::{
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::OneInMessageProcessorT,
    },
    Uuid,
};

/// Message Processor that defines the generation and execution of events for a ParallelOneInTwoOut
/// operator, where
/// O: An operator that implements the ParallelOneInTwoOut trait,
/// S: A state structure that implements the AppendableStateT trait,
/// T: Type of messages received on the read stream,
/// U: Type of messages sent on the left write stream,
/// V: Type of messages sent on the write stream,
/// W: Type of intermediate data appended to the state structure S.
pub struct ParallelOneInTwoOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelOneInTwoOut<S, T, U, V, W>,
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
    W: 'static + Send + Sync,
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

impl<O, S, T, U, V, W> OneInMessageProcessorT<S, T>
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
            let time_copy = time.clone();
            let time_copy_left = time.clone();
            let time_copy_right = time.clone();
            OperatorEvent::new(
                time.clone(),
                true,
                127,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    // Invoke the watermark method.
                    operator.on_watermark(&mut ParallelOneInTwoOutContext::new(
                        time,
                        config,
                        &state,
                        left_write_stream,
                        right_write_stream,
                    ));

                    // Send a watermark.
                    left_write_stream_copy
                        .send(Message::new_watermark(time_copy_left))
                        .ok();
                    right_write_stream_copy
                        .send(Message::new_watermark(time_copy_right))
                        .ok();

                    // Commit the state.
                    state.commit(&time_copy);
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
                    // Invoke the watermark method.
                    operator.on_watermark(&mut ParallelOneInTwoOutContext::new(
                        time.clone(),
                        config,
                        &state,
                        left_write_stream,
                        right_write_stream,
                    ));

                    // Commit the state.
                    state.commit(&time);
                },
                OperatorType::Parallel,
            )
        }
    }

    fn arm_deadlines(
        &self,
        setup_context: &mut SetupContext<S>,
        read_stream_id: StreamId,
        condition_context: &ConditionContext,
        timestamp: Timestamp,
    ) -> Vec<DeadlineEvent> {
        let mut deadline_events = Vec::new();
        let state = Arc::clone(&self.state);
        for deadline in setup_context.get_deadlines() {
            if deadline
                .get_constrained_read_stream_ids()
                .contains(&read_stream_id)
                && deadline.invoke_start_condition(
                    vec![read_stream_id],
                    condition_context,
                    &timestamp,
                )
            {
                // Compute the deadline for the timestamp.
                let deadline_duration = deadline.calculate_deadline(&state, &timestamp);
                deadline_events.push(DeadlineEvent::new(
                    deadline.get_constrained_read_stream_ids().clone(),
                    deadline.get_constrained_write_stream_ids().clone(),
                    timestamp.clone(),
                    deadline_duration,
                    deadline.get_end_condition_fn(),
                    deadline.get_id(),
                ));
            }
        }
        deadline_events
    }

    fn disarm_deadline(&self, deadline_event: &DeadlineEvent) -> bool {
        let left_write_stream_id = self.left_write_stream.id();
        let right_write_stream_id = self.right_write_stream.id();
        if deadline_event
            .write_stream_ids
            .contains(&left_write_stream_id)
            || deadline_event
                .write_stream_ids
                .contains(&right_write_stream_id)
        {
            // Invoke the end condition function on the statistics from the WriteStream.
            return (deadline_event.end_condition)(
                vec![left_write_stream_id, right_write_stream_id],
                &(self
                    .left_write_stream
                    .get_condition_context()
                    .merge(&self.right_write_stream.get_condition_context())),
                &deadline_event.timestamp,
            );
        }
        false
    }

    fn invoke_handler(
        &self,
        setup_context: &mut SetupContext<S>,
        deadline_id: DeadlineId,
        timestamp: Timestamp,
    ) {
        setup_context.invoke_handler(deadline_id, &(*self.state), &timestamp);
    }
}

/// Message Processor that defines the generation and execution of events for a OneInTwoOut
/// operator, where
/// O: An operator that implements the OneInTwoOut trait,
/// S: A state structure that implements the StateT trait,
/// T: Type of messages received on the read stream,
/// U: Type of messages sent on the left write stream,
/// V: Type of messages sent on the right write stream.
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

impl<O, S, T, U, V> OneInMessageProcessorT<S, T> for OneInTwoOutMessageProcessor<O, S, T, U, V>
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
            let time_copy = time.clone();
            let time_copy_left = time.clone();
            let time_copy_right = time.clone();
            OperatorEvent::new(
                time.clone(),
                true,
                127,
                HashSet::new(),
                self.state_ids.clone(),
                move || {
                    // Take a lock on the state and the operator and invoke the callback.
                    let mutable_state = &mut state.lock().unwrap();
                    operator
                        .lock()
                        .unwrap()
                        .on_watermark(&mut OneInTwoOutContext::new(
                            time,
                            config,
                            mutable_state,
                            left_write_stream,
                            right_write_stream,
                        ));

                    // Send a watermark.
                    left_write_stream_copy
                        .send(Message::new_watermark(time_copy_left))
                        .ok();
                    right_write_stream_copy
                        .send(Message::new_watermark(time_copy_right))
                        .ok();

                    // Commit the state.
                    mutable_state.commit(&time_copy);
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
                    // Take a lock on the state and the operator and invoke the callback.
                    let mutable_state = &mut state.lock().unwrap();
                    operator
                        .lock()
                        .unwrap()
                        .on_watermark(&mut OneInTwoOutContext::new(
                            time.clone(),
                            config,
                            &mut state.lock().unwrap(),
                            left_write_stream,
                            right_write_stream,
                        ));

                    // Commit the state.
                    mutable_state.commit(&time);
                },
                OperatorType::Sequential,
            )
        }
    }

    fn arm_deadlines(
        &self,
        setup_context: &mut SetupContext<S>,
        read_stream_id: StreamId,
        condition_context: &ConditionContext,
        timestamp: Timestamp,
    ) -> Vec<DeadlineEvent> {
        let mut deadline_events = Vec::new();
        let state = Arc::clone(&self.state);
        for deadline in setup_context.get_deadlines() {
            if deadline
                .get_constrained_read_stream_ids()
                .contains(&read_stream_id)
                && deadline.invoke_start_condition(
                    vec![read_stream_id],
                    condition_context,
                    &timestamp,
                )
            {
                // Compute the deadline for the timestamp.
                let deadline_duration =
                    deadline.calculate_deadline(&(*state.lock().unwrap()), &timestamp);
                deadline_events.push(DeadlineEvent::new(
                    deadline.get_constrained_read_stream_ids().clone(),
                    deadline.get_constrained_write_stream_ids().clone(),
                    timestamp.clone(),
                    deadline_duration,
                    deadline.get_end_condition_fn(),
                    deadline.get_id(),
                ));
            }
        }
        deadline_events
    }

    fn disarm_deadline(&self, deadline_event: &DeadlineEvent) -> bool {
        let left_write_stream_id = self.left_write_stream.id();
        let right_write_stream_id = self.right_write_stream.id();
        if deadline_event
            .write_stream_ids
            .contains(&left_write_stream_id)
            || deadline_event
                .write_stream_ids
                .contains(&right_write_stream_id)
        {
            // Invoke the end condition function on the statistics from the WriteStream.
            return (deadline_event.end_condition)(
                vec![left_write_stream_id, right_write_stream_id],
                &(self
                    .left_write_stream
                    .get_condition_context()
                    .merge(&self.right_write_stream.get_condition_context())),
                &deadline_event.timestamp,
            );
        }
        false
    }

    fn invoke_handler(
        &self,
        setup_context: &mut SetupContext<S>,
        deadline_id: DeadlineId,
        timestamp: Timestamp,
    ) {
        setup_context.invoke_handler(deadline_id, &(*self.state.lock().unwrap()), &timestamp);
    }
}
