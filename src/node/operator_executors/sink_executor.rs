use serde::Deserialize;
use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    dataflow::{
        context::{ParallelSinkContext, SinkContext},
        operator::{OperatorConfig, ParallelSink, Sink},
        AppendableStateT, Data, Message, ReadStream, StateT, Timestamp,
    },
    node::{
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::OneInMessageProcessorT,
    },
    Uuid,
};

/// Message Processor that defines the generation and execution of events for a ParallelSink
/// operator, where
/// O: An operator that implements the ParallelSink trait,
/// S: A state structure that implements the AppendableStateT trait,
/// T: Type of messages received on the read stream,
/// U: Type of intermediate data appended to the state structure S.
pub struct ParallelSinkMessageProcessor<O, S, T, U>
where
    O: 'static + ParallelSink<S, T, U>,
    S: AppendableStateT<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
{
    config: OperatorConfig,
    operator: Arc<O>,
    state: Arc<S>,
    state_ids: HashSet<Uuid>,
    phantom_t: PhantomData<T>,
    phantom_u: PhantomData<U>,
}

impl<O, S, T, U> ParallelSinkMessageProcessor<O, S, T, U>
where
    O: 'static + ParallelSink<S, T, U>,
    S: AppendableStateT<U>,
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
            operator: Arc::new(operator_fn()),
            state: Arc::new(state_fn()),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            phantom_t: PhantomData,
            phantom_u: PhantomData,
        }
    }
}

impl<O, S, T, U> OneInMessageProcessorT<T> for ParallelSinkMessageProcessor<O, S, T, U>
where
    O: 'static + ParallelSink<S, T, U>,
    S: AppendableStateT<U>,
    T: Data + for<'a> Deserialize<'a>,
    U: 'static + Send + Sync,
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
                    &ParallelSinkContext::new(time, config, &state),
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

        OperatorEvent::new(
            time.clone(),
            true,
            0,
            HashSet::new(),
            self.state_ids.clone(),
            move || {
                // Invoke the watermark method.
                operator.on_watermark(&mut ParallelSinkContext::new(time.clone(), config, &state));

                // Commit the state
                state.commit(&time);
            },
            OperatorType::Parallel,
        )
    }
}

/// Message Processor that defines the generation and execution of events for a Sink operator,
/// where
/// O: An operator that implements the Sink trait,
/// S: A state structure that implements the StateT trait,
/// T: Type of messages received on the read stream,
pub struct SinkMessageProcessor<O, S, T>
where
    O: 'static + Sink<S, T>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: Arc<Mutex<O>>,
    state: Arc<Mutex<S>>,
    state_ids: HashSet<Uuid>,
    phantom_t: PhantomData<T>,
}

impl<O, S, T> SinkMessageProcessor<O, S, T>
where
    O: 'static + Sink<S, T>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
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
        }
    }
}

impl<O, S, T> OneInMessageProcessorT<T> for SinkMessageProcessor<O, S, T>
where
    O: 'static + Sink<S, T>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
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
                    &mut SinkContext::new(time, config, &mut state.lock().unwrap()),
                    msg.data().unwrap(),
                )
            },
            OperatorType::Sequential,
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
                // Take a lock on the state and the operator and invoke the callback.
                let mutable_state = &mut state.lock().unwrap();
                operator.lock().unwrap().on_watermark(&mut SinkContext::new(
                    time.clone(),
                    config,
                    mutable_state,
                ));

                // Commit the state.
                mutable_state.commit(&time);
            },
            OperatorType::Sequential,
        )
    }
}
