use serde::Deserialize;
use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    dataflow::{
        operator::{OperatorConfig, ParallelSink, ParallelSinkContext, Sink, SinkContext},
        Data, Message, ReadOnlyState, ReadStream, Timestamp, WriteableState,
    },
    node::{
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::OneInMessageProcessorT,
    },
    Uuid,
};

pub struct ParallelSinkMessageProcessor<O, S, T, U, V>
where
    O: 'static + ParallelSink<S, T, U, V>,
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

impl<O, S, T, U, V> ParallelSinkMessageProcessor<O, S, T, U, V>
where
    O: 'static + ParallelSink<S, T, U, V>,
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

impl<O, S, T, U, V> OneInMessageProcessorT<T> for ParallelSinkMessageProcessor<O, S, T, U, V>
where
    O: 'static + ParallelSink<S, T, U, V>,
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
            move || operator.on_watermark(&mut ParallelSinkContext::new(time, config, &state)),
            OperatorType::Parallel,
        )
    }
}

pub struct SinkMessageProcessor<O, S, T, U>
where
    O: 'static + Sink<S, T, U>,
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

impl<O, S, T, U> SinkMessageProcessor<O, S, T, U>
where
    O: 'static + Sink<S, T, U>,
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

impl<O, S, T, U> OneInMessageProcessorT<T> for SinkMessageProcessor<O, S, T, U>
where
    O: 'static + Sink<S, T, U>,
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
                operator.lock().unwrap().on_watermark(&mut SinkContext::new(
                    time,
                    config,
                    &mut state.lock().unwrap(),
                ))
            },
            OperatorType::Sequential,
        )
    }
}
