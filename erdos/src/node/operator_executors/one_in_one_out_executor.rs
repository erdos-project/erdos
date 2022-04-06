use serde::Deserialize;
use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    dataflow::{
        context::{OneInOneOutContext, ParallelOneInOneOutContext, SetupContext},
        deadlines::{ConditionContext, DeadlineEvent, DeadlineId},
        operator::{OneInOneOut, OperatorConfig, ParallelOneInOneOut},
        stream::{StreamId, WriteStreamT},
        AppendableState, Data, Message, ReadStream, State, Timestamp, WriteStream,
    },
    node::{
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::OneInMessageProcessorT,
    },
    Uuid,
};

/// Message Processor that defines the generation and execution of events for a ParallelOneInOneOut
/// operator, where
/// O: An operator that implements the ParallelOneInOneOut trait,
/// S: A state structure that implements the AppendableState trait,
/// T: Type of messages received on the read stream,
/// U: Type of messages sent on the write stream,
/// V: Type of intermediate data appended to the state structure S.
pub struct ParallelOneInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + ParallelOneInOneOut<S, T, U, V>,
    S: AppendableState<V>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: 'static + Send + Sync,
{
    config: OperatorConfig,
    operator: Arc<O>,
    state: Arc<S>,
    state_ids: HashSet<Uuid>,
    write_stream: WriteStream<U>,
    phantom_t: PhantomData<T>,
    phantom_v: PhantomData<V>,
}

impl<O, S, T, U, V> ParallelOneInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + ParallelOneInOneOut<S, T, U, V>,
    S: AppendableState<V>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: 'static + Send + Sync,
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
        }
    }
}

impl<O, S, T, U, V> OneInMessageProcessorT<S, T>
    for ParallelOneInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + ParallelOneInOneOut<S, T, U, V>,
    S: AppendableState<V>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: 'static + Send + Sync,
{
    fn execute_setup(&mut self, read_stream: &mut ReadStream<T>) -> SetupContext<S> {
        let mut setup_context =
            SetupContext::new(vec![read_stream.id()], vec![self.write_stream.id()]);
        Arc::get_mut(&mut self.operator)
            .unwrap()
            .setup(&mut setup_context);
        setup_context
    }

    fn execute_run(&mut self, read_stream: &mut ReadStream<T>) {
        Arc::get_mut(&mut self.operator).unwrap().run(
            &self.config,
            read_stream,
            &mut self.write_stream,
        );
    }

    fn execute_destroy(&mut self) {
        Arc::get_mut(&mut self.operator).unwrap().destroy();
    }

    fn cleanup(&mut self) {
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .unwrap_or_else(|_| {
                    panic!(
                        "[ParallelOneInOneOut] Error sending Top watermark for operator {}",
                        self.config.get_name()
                    )
                });
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
                    // Invoke the watermark method.
                    operator.on_watermark(&mut ParallelOneInOneOutContext::new(
                        time,
                        config,
                        &state,
                        write_stream,
                    ));

                    // Send a watermark.
                    write_stream_copy
                        .send(Message::new_watermark(time_copy.clone()))
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
                    operator.on_watermark(&mut ParallelOneInOneOutContext::new(
                        time.clone(),
                        config,
                        &state,
                        write_stream,
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
        read_stream_ids: Vec<StreamId>,
        condition_context: &ConditionContext,
        timestamp: Timestamp,
    ) -> Vec<DeadlineEvent> {
        let mut deadline_events = Vec::new();
        let state = Arc::clone(&self.state);
        for deadline in setup_context.deadlines() {
            if deadline
                .get_constrained_read_stream_ids()
                .is_superset(&read_stream_ids.iter().cloned().collect())
                && deadline.invoke_start_condition(&read_stream_ids, condition_context, &timestamp)
            {
                // Compute the deadline for the timestamp.
                let deadline_duration = deadline.calculate_deadline(&state, &timestamp);
                deadline_events.push(DeadlineEvent::new(
                    deadline.get_constrained_read_stream_ids().clone(),
                    deadline.get_constrained_write_stream_ids().clone(),
                    timestamp.clone(),
                    deadline_duration,
                    deadline.get_end_condition_fn(),
                    deadline.id(),
                ));
            }
        }
        deadline_events
    }

    fn disarm_deadline(&self, deadline_event: &DeadlineEvent) -> bool {
        let write_stream_id = self.write_stream.id();
        if deadline_event.write_stream_ids.contains(&write_stream_id) {
            // Invoke the end condition function on the statistics from the WriteStream.
            return (deadline_event.end_condition)(
                &[write_stream_id],
                &self.write_stream.get_condition_context(),
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

/// Message Processor that defines the generation and execution of events for a OneInOneOut
/// operator, where
/// O: An operator that implements the OneInOneOut trait,
/// S: A state structure that implements the State trait,
/// T: Type of messages received on the read stream,
/// U: Type of messages sent on the write stream,
pub struct OneInOneOutMessageProcessor<O, S, T, U>
where
    O: 'static + OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: Arc<Mutex<O>>,
    state: Arc<Mutex<S>>,
    state_ids: HashSet<Uuid>,
    write_stream: WriteStream<U>,
    phantom_t: PhantomData<T>,
}

impl<O, S, T, U> OneInOneOutMessageProcessor<O, S, T, U>
where
    O: 'static + OneInOneOut<S, T, U>,
    S: State,
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
        }
    }
}

impl<O, S, T, U> OneInMessageProcessorT<S, T> for OneInOneOutMessageProcessor<O, S, T, U>
where
    O: 'static + OneInOneOut<S, T, U>,
    S: State,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn execute_setup(&mut self, read_stream: &mut ReadStream<T>) -> SetupContext<S> {
        let mut setup_context =
            SetupContext::new(vec![read_stream.id()], vec![self.write_stream.id()]);
        self.operator.lock().unwrap().setup(&mut setup_context);
        setup_context
    }

    fn execute_run(&mut self, read_stream: &mut ReadStream<T>) {
        self.operator
            .lock()
            .unwrap()
            .run(&self.config, read_stream, &mut self.write_stream);
    }

    fn execute_destroy(&mut self) {
        self.operator.lock().unwrap().destroy();
    }

    fn cleanup(&mut self) {
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .unwrap_or_else(|_| {
                    panic!(
                        "[OneInOneOut] Error sending Top watermark for operator {}",
                        self.config.get_name()
                    )
                });
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
                // Note: to avoid deadlock, always lock the operator before the state.
                let mut mutable_operator = operator.lock().unwrap();
                let mut mutable_state = state.lock().unwrap();

                mutable_operator.on_data(
                    &mut OneInOneOutContext::new(time, config, &mut mutable_state, write_stream),
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
                    // Note: to avoid deadlock, always lock the operator before the state.
                    let mut mutable_operator = operator.lock().unwrap();
                    let mut mutable_state = state.lock().unwrap();

                    mutable_operator.on_watermark(&mut OneInOneOutContext::new(
                        time,
                        config,
                        &mut mutable_state,
                        write_stream,
                    ));

                    // Send a watermark.
                    write_stream_copy
                        .send(Message::new_watermark(time_copy.clone()))
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
                    // Note: to avoid deadlock, always lock the operator before the state.
                    let mut mutable_operator = operator.lock().unwrap();
                    let mut mutable_state = state.lock().unwrap();

                    mutable_operator.on_watermark(&mut OneInOneOutContext::new(
                        time.clone(),
                        config,
                        &mut mutable_state,
                        write_stream,
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
        read_stream_ids: Vec<StreamId>,
        condition_context: &ConditionContext,
        timestamp: Timestamp,
    ) -> Vec<DeadlineEvent> {
        let mut deadline_events = Vec::new();
        let state = Arc::clone(&self.state);
        for deadline in setup_context.deadlines() {
            if deadline
                .get_constrained_read_stream_ids()
                .is_superset(&read_stream_ids.iter().cloned().collect())
                && deadline.invoke_start_condition(&read_stream_ids, condition_context, &timestamp)
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
                    deadline.id(),
                ));
            }
        }
        deadline_events
    }

    fn disarm_deadline(&self, deadline_event: &DeadlineEvent) -> bool {
        let write_stream_id = self.write_stream.id();
        if deadline_event.write_stream_ids.contains(&write_stream_id) {
            // Invoke the end condition function on the statistics from the WriteStream.
            return (deadline_event.end_condition)(
                &[write_stream_id],
                &self.write_stream.get_condition_context(),
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
