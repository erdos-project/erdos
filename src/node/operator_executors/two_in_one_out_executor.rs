use serde::Deserialize;
use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    dataflow::{
        operator::{
            OperatorConfig, ParallelTwoInOneOut, ParallelTwoInOneOutContext, TwoInOneOut,
            TwoInOneOutContext,
        },
        stream::WriteStreamT,
        AppendableStateT, Data, Message, ReadStream, StateT, Timestamp, WriteStream,
    },
    node::{
        operator_event::{OperatorEvent, OperatorType},
        operator_executors::TwoInMessageProcessorT,
    },
    Uuid,
};

/// Message Processor that defines the generation and execution of events for a ParallelTwoInOneOut
/// operator, where
/// O: An operator that implements the ParallelTwoInOneOut trait,
/// S: A state structure that implements the AppendableStateT trait,
/// T: Type of messages received on the left read stream,
/// U: Type of messages received on the right read stream,
/// V: Type of messages sent on the write stream,
/// W: Type of intermediate data appended to the state structure S.
pub struct ParallelTwoInOneOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelTwoInOneOut<S, T, U, V, W>,
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
    write_stream: WriteStream<V>,
    phantom_t: PhantomData<T>,
    phantom_u: PhantomData<U>,
    phantom_w: PhantomData<W>,
}

impl<O, S, T, U, V, W> ParallelTwoInOneOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelTwoInOneOut<S, T, U, V, W>,
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
        write_stream: WriteStream<V>,
    ) -> Self {
        Self {
            config,
            operator: Arc::new(operator_fn()),
            state: Arc::new(state_fn()),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            write_stream,
            phantom_t: PhantomData,
            phantom_u: PhantomData,
            phantom_w: PhantomData,
        }
    }
}

impl<O, S, T, U, V, W> TwoInMessageProcessorT<T, U>
    for ParallelTwoInOneOutMessageProcessor<O, S, T, U, V, W>
where
    O: 'static + ParallelTwoInOneOut<S, T, U, V, W>,
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
    W: 'static + Send + Sync,
{
    fn execute_run(
        &mut self,
        left_read_stream: &mut ReadStream<T>,
        right_read_stream: &mut ReadStream<U>,
    ) {
        Arc::get_mut(&mut self.operator).unwrap().run(
            left_read_stream,
            right_read_stream,
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
                .expect(&format!(
                    "[ParallelTwoInOneOut] Error sending Top watermark for operator {}",
                    self.config.get_name(),
                ));
        }
    }

    fn left_message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
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
                operator.on_left_data(
                    &ParallelTwoInOneOutContext::new(time, config, &state, write_stream),
                    msg.data().unwrap(),
                )
            },
            OperatorType::Parallel,
        )
    }

    fn right_message_cb_event(&mut self, msg: Arc<Message<U>>) -> OperatorEvent {
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
                operator.on_right_data(
                    &ParallelTwoInOneOutContext::new(time, config, &state, write_stream),
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
                    operator.on_watermark(&mut ParallelTwoInOneOutContext::new(
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
                    operator.on_watermark(&mut ParallelTwoInOneOutContext::new(
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
}

/// Message Processor that defines the generation and execution of events for a TwoInOneOut
/// operator, where
/// O: An operator that implements the TwoInOneOut trait,
/// S: A state structure that implements the StateT trait,
/// T: Type of messages received on the left read stream,
/// U: Type of messages received on the right read stream,
/// V: Type of messages sent on the write stream,
pub struct TwoInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + TwoInOneOut<S, T, U, V>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    config: OperatorConfig,
    operator: Arc<Mutex<O>>,
    state: Arc<Mutex<S>>,
    state_ids: HashSet<Uuid>,
    write_stream: WriteStream<V>,
    phantom_t: PhantomData<T>,
    phantom_u: PhantomData<U>,
}

impl<O, S, T, U, V> TwoInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + TwoInOneOut<S, T, U, V>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    pub fn new(
        config: OperatorConfig,
        operator_fn: impl Fn() -> O + Send,
        state_fn: impl Fn() -> S + Send,
        write_stream: WriteStream<V>,
    ) -> Self {
        Self {
            config,
            operator: Arc::new(Mutex::new(operator_fn())),
            state: Arc::new(Mutex::new(state_fn())),
            state_ids: vec![Uuid::new_deterministic()].into_iter().collect(),
            write_stream,
            phantom_t: PhantomData,
            phantom_u: PhantomData,
        }
    }
}

impl<O, S, T, U, V> TwoInMessageProcessorT<T, U> for TwoInOneOutMessageProcessor<O, S, T, U, V>
where
    O: 'static + TwoInOneOut<S, T, U, V>,
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn execute_run(
        &mut self,
        left_read_stream: &mut ReadStream<T>,
        right_read_stream: &mut ReadStream<U>,
    ) {
        self.operator.lock().unwrap().run(
            left_read_stream,
            right_read_stream,
            &mut self.write_stream,
        );
    }

    fn execute_destroy(&mut self) {
        self.operator.lock().unwrap().destroy();
    }

    fn cleanup(&mut self) {
        if !self.write_stream.is_closed() {
            self.write_stream
                .send(Message::new_watermark(Timestamp::Top))
                .expect(&format!(
                    "[TwoInOneOut] Error sending Top watermark for operator {}",
                    self.config.get_name(),
                ));
        }
    }

    fn left_message_cb_event(&mut self, msg: Arc<Message<T>>) -> OperatorEvent {
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
                operator.lock().unwrap().on_left_data(
                    &mut TwoInOneOutContext::new(
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

    fn right_message_cb_event(&mut self, msg: Arc<Message<U>>) -> OperatorEvent {
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
                operator.lock().unwrap().on_right_data(
                    &mut TwoInOneOutContext::new(
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
                    // Take a lock on the state and the operator and invoke the callback.
                    let mutable_state = &mut state.lock().unwrap();
                    operator
                        .lock()
                        .unwrap()
                        .on_watermark(&mut TwoInOneOutContext::new(
                            time,
                            config,
                            mutable_state,
                            write_stream,
                        ));

                    // Send a watermark
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
                    // Take a lock on the state and the operator and invoke the callback.
                    let mutable_state = &mut state.lock().unwrap();
                    operator
                        .lock()
                        .unwrap()
                        .on_watermark(&mut TwoInOneOutContext::new(
                            time.clone(),
                            config,
                            mutable_state,
                            write_stream,
                        ));

                    // Commit the state.
                    mutable_state.commit(&time);
                },
                OperatorType::Sequential,
            )
        }
    }
}
