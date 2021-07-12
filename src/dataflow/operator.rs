use std::{marker::PhantomData, slice::Iter};

use serde::Deserialize;

use crate::{
    dataflow::{
        deadlines::Deadline, stream::StreamId, AppendableStateT, Data, ReadStream, State, StateT,
        Timestamp, WriteStream,
    },
    node::NodeId,
    OperatorId,
};

pub trait SetupContextT: Send + Sync {
    fn add_deadline(&mut self, deadline: Deadline);
    fn get_deadlines(&self) -> Iter<Deadline>;
    fn num_deadlines(&self) -> usize;
}

/*************************************************************************************************
 * Source: sends data with type T                                                                *
 ************************************************************************************************/

/// The `Source` trait must be implemented by operators that generate data for the rest of the
/// dataflow graph. Specifically, such an operator does not take an input read stream, but
/// generates data on an output write stream. The operator must take control of its execution by
/// implementing the `run` method, and generating output on the `write_stream` passed to it.
#[allow(unused_variables)]
pub trait Source<S, T>: Send + Sync
where
    S: State,
    T: Data + for<'a> Deserialize<'a>,
{
    fn run(&mut self, write_stream: &mut WriteStream<T>) {}

    fn destroy(&mut self) {}
}

/*************************************************************************************************
 * ParallelSink: receives data with type T, and enables message callbacks to execute in parallel *
 * by only allowing append access in the message callbacks and writable access to the state in   *
 * the watermark callbacks.                                                                      *
 ************************************************************************************************/

/// The `ParallelSink` trait must be implemented by operators that consume data from their input
/// read stream, and do not generate any output. The operator can either choose to consume data
/// from its `read_stream` itself using the `run` method, or register for the `on_data` and
/// `on_watermark` callbacks.
///
/// This trait differs from the `Sink` trait by allowing the `on_data` message callbacks to execute
/// in parallel and append intermediate data to a state structure that can be utilized by the
/// `on_watermark` method to generate the final, complete data. Note that while the message
/// callbacks can execute in any order, the watermark callbacks execute sequentially and are
/// ordered by the timestamp order.
#[allow(unused_variables)]
pub trait ParallelSink<S: AppendableStateT<U>, T: Data, U>: Send + Sync {
    fn run(&mut self, read_stream: &mut ReadStream<T>) {}

    fn destroy(&mut self) {}

    fn on_data(&self, ctx: &ParallelSinkContext<S, U>, data: &T);

    fn on_watermark(&self, ctx: &mut ParallelSinkContext<S, U>);
}

/// A context structure made available to the callbacks of a `ParallelSink` operator. The context
/// provides access to the current timestamp for which the callback is invoked along with the state
/// of the operator.
pub struct ParallelSinkContext<'a, S: AppendableStateT<T>, T> {
    timestamp: Timestamp,
    config: OperatorConfig,
    state: &'a S,
    phantomdata_t: PhantomData<T>,
}

impl<'a, S, T> ParallelSinkContext<'a, S, T>
where
    S: 'static + AppendableStateT<T>,
{
    pub fn new(timestamp: Timestamp, config: OperatorConfig, state: &'a S) -> Self {
        Self {
            timestamp,
            config,
            state,
            phantomdata_t: PhantomData,
        }
    }

    /// Get the timestamp for which the callback was invoked.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Get the configuration of the operator.
    pub fn get_operator_config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Get the state attached to the operator.
    pub fn get_state(&self) -> &S {
        &self.state
    }
}

/*************************************************************************************************
 * Sink: receives data with type T, and enables message callbacks to have mutable access to the  *
 * operator state, but all callbacks are sequentialized.                                         *
 ************************************************************************************************/

/// The `Sink` trait must be implemented by operators that consume data from their input read
/// stream, and do not generate any output. The operator can either choose to consume data
/// from its `read_stream` itself using the `run` method, or register for the `on_data` and
/// `on_watermark` callbacks.
///
/// The callbacks registered in this operator execute sequentially by timestamp order and are
/// allowed to mutate state in both the message and the watermark callbacks.
#[allow(unused_variables)]
pub trait Sink<S: StateT, T: Data>: Send + Sync {
    fn run(&mut self, read_stream: &mut ReadStream<T>) {}

    fn destroy(&mut self) {}

    fn on_data(&mut self, ctx: &mut SinkContext<S>, data: &T);

    fn on_watermark(&mut self, ctx: &mut SinkContext<S>);
}

/// A context structure made available to the callbacks of a `Sink` operator. The context provides
/// access to the current timestamp for which the callback is invoked along with the state
/// of the operator.
pub struct SinkContext<'a, S: StateT> {
    timestamp: Timestamp,
    config: OperatorConfig,
    state: &'a mut S,
}

impl<'a, S> SinkContext<'a, S>
where
    S: StateT,
{
    pub fn new(timestamp: Timestamp, config: OperatorConfig, state: &'a mut S) -> Self {
        Self {
            timestamp,
            config,
            state,
        }
    }

    /// Get the timestamp for which the callback was invoked.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Get the configuration of the operator.
    pub fn get_operator_config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Get the state attached to the operator.
    pub fn get_state(&mut self) -> &mut S {
        &mut self.state
    }
}

/*************************************************************************************************
 * ParallelOneInOneOut: Receives data with type T, and enables message callbacks to execute in   *
 * parallel by allowing append access in the message callbacks, and writable access to the state *
 * in the watermark along with the ability to send messages on the write stream.                 *
 ************************************************************************************************/

/// The `ParallelOneInOneOut` trait must be implemented by operators that consume data from their
/// input read stream, and generate output on an output write stream. The operator can either
/// choose to consume data from its `read_stream` itself using the `run` method, or register for
/// the `on_data` and `on_watermark` callbacks.
///
/// This trait differs from the `OneInOneOut` trait by allowing the `on_data` message callbacks to
/// execute in parallel and append intermediate data to a state structure that can be utilized by
/// the `on_watermark` method to generate the final, complete data. Note that while the message
/// callbacks can execute in any order, the watermark callbacks execute sequentially and are
/// ordered by the timestamp order.
#[allow(unused_variables)]
pub trait ParallelOneInOneOut<S, T, U, V>: Send + Sync
where
    S: AppendableStateT<V>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn run(&mut self, read_stream: &mut ReadStream<T>, write_stream: &mut WriteStream<U>) {}

    fn destroy(&mut self) {}

    fn on_data(&self, ctx: &ParallelOneInOneOutContext<S, U, V>, data: &T);

    fn on_watermark(&self, ctx: &mut ParallelOneInOneOutContext<S, U, V>);
}

/// A context structure made available to the callbacks of a `ParallelOneInOneOut` operator. The
/// context provides access to the current timestamp for which the callback is invoked along with
/// the state of the operator and the write stream to send the outputs on.
pub struct ParallelOneInOneOutContext<'a, S, T, U>
where
    S: AppendableStateT<U>,
    T: Data + for<'b> Deserialize<'b>,
{
    timestamp: Timestamp,
    config: OperatorConfig,
    state: &'a S,
    write_stream: WriteStream<T>,
    phantom_u: PhantomData<U>,
}

impl<'a, S, T, U> ParallelOneInOneOutContext<'a, S, T, U>
where
    S: AppendableStateT<U>,
    T: Data + for<'b> Deserialize<'b>,
{
    pub fn new(
        timestamp: Timestamp,
        config: OperatorConfig,
        state: &'a S,
        write_stream: WriteStream<T>,
    ) -> Self {
        Self {
            timestamp,
            config,
            state,
            write_stream,
            phantom_u: PhantomData,
        }
    }

    /// Get the timestamp for which the callback was invoked.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Get the configuration of the operator.
    pub fn get_operator_config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Get the state attached to the operator.
    pub fn get_state(&self) -> &S {
        &self.state
    }

    /// Get the write stream to send the output on.
    pub fn get_write_stream(&mut self) -> &mut WriteStream<T> {
        &mut self.write_stream
    }
}

/**************************************************************************************************
 * OneInOneOut: Receives data with type T, and enables message callbacks to have mutable access   *
 * to the operator state, but all callbacks are sequentialized.                                   *
 *************************************************************************************************/

/// The `OneInOneOut` trait must be implemented by operators that consume data from their input
/// read stream, and generate output on an output write stream. The operator can either choose to
/// consume data from its `read_stream` itself using the `run` method, or register for the
/// `on_data` and `on_watermark` callbacks.
///
/// The callbacks registered in this operator execute sequentially by timestamp order and are
/// allowed to mutate state in both the message and the watermark callbacks.
#[allow(unused_variables)]
pub trait OneInOneOut<S, T, U>: Send + Sync
where
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn run(&mut self, read_stream: &mut ReadStream<T>, write_stream: &mut WriteStream<U>) {}

    fn destroy(&mut self) {}

    fn on_data(&mut self, ctx: &mut OneInOneOutContext<S, U>, data: &T);

    fn on_watermark(&mut self, ctx: &mut OneInOneOutContext<S, U>);
}

/// A context structure made available to the callbacks of a `OneInOneOut` operator. The context
/// provides access to the current timestamp for which the callback is invoked along with the
/// state of the operator and the write stream to send the outputs on.
pub struct OneInOneOutContext<'a, S, T>
where
    S: StateT,
    T: Data + for<'b> Deserialize<'b>,
{
    timestamp: Timestamp,
    config: OperatorConfig,
    state: &'a mut S,
    write_stream: WriteStream<T>,
}

impl<'a, S, T> OneInOneOutContext<'a, S, T>
where
    S: StateT,
    T: Data + for<'b> Deserialize<'b>,
{
    pub fn new(
        timestamp: Timestamp,
        config: OperatorConfig,
        state: &'a mut S,
        write_stream: WriteStream<T>,
    ) -> Self {
        Self {
            timestamp,
            config,
            state,
            write_stream,
        }
    }

    /// Get the timestamp for which the callback was invoked.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Get the configuration of the operator.
    pub fn get_operator_config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Get the state attached to the operator.
    pub fn get_state(&mut self) -> &mut S {
        &mut self.state
    }

    /// Get the write stream to send the output on.
    pub fn get_write_stream(&mut self) -> &mut WriteStream<T> {
        &mut self.write_stream
    }
}

pub struct OneInOneOutSetupContext {
    pub(crate) deadlines: Vec<Deadline>,
    pub read_stream_id: StreamId,
}

impl OneInOneOutSetupContext {
    pub fn new(read_stream_id: StreamId) -> Self {
        OneInOneOutSetupContext {
            deadlines: Vec::new(),
            read_stream_id,
        }
    }
}

impl SetupContextT for OneInOneOutSetupContext {
    fn add_deadline(&mut self, deadline: Deadline) {
        self.deadlines.push(deadline);
    }

    fn get_deadlines(&self) -> Iter<Deadline> {
        self.deadlines.iter()
    }

    fn num_deadlines(&self) -> usize {
        self.deadlines.len()
    }
}

/*************************************************************************************************
 * ParallelTwoInOneOut: Receives data with type T and U, and enables message callbacks to        *
 * execute in parallel by allowing append access in the message callbacks, and writable access   *
 * to the state in the watermark along with the ability to send messages on the write stream.    *
 ************************************************************************************************/

/// The `ParallelTwoInOneOut` trait must be implemented by operators that consume data from two
/// input read streams, and generate output on an output write stream. The operator can either
/// choose to consume data from its `read_stream` itself using the `run` method, or register for
/// the `on_data` and `on_watermark` callbacks.
///
/// This trait differs from the `TwoInOneOut` trait by allowing the `on_data` message callbacks to
/// execute in parallel and append intermediate data to a state structure that can be utilized by
/// the `on_watermark` method to generate the final, complete data. Note that while the message
/// callbacks can execute in any order, the watermark callbacks execute sequentially and are
/// ordered by the timestamp order.
#[allow(unused_variables)]
pub trait ParallelTwoInOneOut<S, T, U, V, W>: Send + Sync
where
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn run(
        &mut self,
        left_read_stream: &mut ReadStream<T>,
        right_read_stream: &mut ReadStream<U>,
        write_stream: &mut WriteStream<V>,
    ) {
    }

    fn destroy(&mut self) {}

    fn on_left_data(&self, ctx: &ParallelTwoInOneOutContext<S, V, W>, data: &T);

    fn on_right_data(&self, ctx: &ParallelTwoInOneOutContext<S, V, W>, data: &U);

    fn on_watermark(&self, ctx: &mut ParallelTwoInOneOutContext<S, V, W>);
}

/// A context structure made available to the callbacks of a `ParallelTwoInOneOut` operator. The
/// context provides access to the current timestamp for which the callback is invoked along with
/// the state of the operator and the write stream to send the outputs on.
pub struct ParallelTwoInOneOutContext<'a, S, T, U>
where
    S: AppendableStateT<U>,
    T: Data + for<'b> Deserialize<'b>,
{
    timestamp: Timestamp,
    config: OperatorConfig,
    state: &'a S,
    write_stream: WriteStream<T>,
    phantom_u: PhantomData<U>,
}

impl<'a, S, T, U> ParallelTwoInOneOutContext<'a, S, T, U>
where
    S: AppendableStateT<U>,
    T: Data + for<'b> Deserialize<'b>,
{
    pub fn new(
        timestamp: Timestamp,
        config: OperatorConfig,
        state: &'a S,
        write_stream: WriteStream<T>,
    ) -> Self {
        Self {
            timestamp,
            config,
            state,
            write_stream,
            phantom_u: PhantomData,
        }
    }

    /// Get the timestamp for which the callback was invoked.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Get the configuration of the operator.
    pub fn get_operator_config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Get the state attached to the operator.
    pub fn get_state(&self) -> &S {
        &self.state
    }

    /// Get the write stream to send the output on.
    pub fn get_write_stream(&mut self) -> &mut WriteStream<T> {
        &mut self.write_stream
    }
}

/**************************************************************************************************
 * TwoInOneOut: Receives data with type T and U, and enables message callbacks to have mutable    *
 * access to the operator state, but all callbacks are sequentialized.                            *
 *************************************************************************************************/

/// The `TwoInOneOut` trait must be implemented by operators that consume data from their input
/// read streams, and generate output on an output write stream. The operator can either choose to
/// consume data from its `read_stream` itself using the `run` method, or register for the
/// `on_data` and `on_watermark` callbacks.
///
/// The callbacks registered in this operator execute sequentially by timestamp order and are
/// allowed to mutate state in both the message and the watermark callbacks.
#[allow(unused_variables)]
pub trait TwoInOneOut<S, T, U, V>: Send + Sync
where
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn run(
        &mut self,
        left_read_stream: &mut ReadStream<T>,
        right_read_stream: &mut ReadStream<U>,
        write_stream: &mut WriteStream<V>,
    ) {
    }

    fn destroy(&mut self) {}

    fn on_left_data(&mut self, ctx: &mut TwoInOneOutContext<S, V>, data: &T);

    fn on_right_data(&mut self, ctx: &mut TwoInOneOutContext<S, V>, data: &U);

    fn on_watermark(&mut self, ctx: &mut TwoInOneOutContext<S, V>);
}

/// A context structure made available to the callbacks of a `TwoInOneOut` operator. The context
/// provides access to the current timestamp for which the callback is invoked along with the
/// state of the operator and the write stream to send the outputs on.
pub struct TwoInOneOutContext<'a, S, T>
where
    S: StateT,
    T: Data + for<'b> Deserialize<'b>,
{
    timestamp: Timestamp,
    config: OperatorConfig,
    state: &'a mut S,
    write_stream: WriteStream<T>,
}

impl<'a, S, T> TwoInOneOutContext<'a, S, T>
where
    S: StateT,
    T: Data + for<'b> Deserialize<'b>,
{
    pub fn new(
        timestamp: Timestamp,
        config: OperatorConfig,
        state: &'a mut S,
        write_stream: WriteStream<T>,
    ) -> Self {
        Self {
            timestamp,
            config,
            state,
            write_stream,
        }
    }

    /// Get the timestamp for which the callback was invoked.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Get the configuration of the operator.
    pub fn get_operator_config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Get the state attached to the operator.
    pub fn get_state(&mut self) -> &mut S {
        &mut self.state
    }

    /// Get the write stream to send the output on.
    pub fn get_write_stream(&mut self) -> &mut WriteStream<T> {
        &mut self.write_stream
    }
}

/*************************************************************************************************
 * ParallelOneInTwoOut: receives data with type T, and enables message callbacks to execute in   *
 * parallel by only allowing append access in the message callbacks and writable access to the   *
 * state in the watermark callbacks.                                                             *
 ************************************************************************************************/

/// The `ParallelOneInTwoOut` trait must be implemented by operators that consume data from their
/// input read stream, and generate output on two output write streams. The operator can either
/// choose to consume data from its `read_stream` itself using the `run` method, or register for
/// the `on_data` and `on_watermark` callbacks.
///
/// This trait differs from the `OneInTwoOut` trait by allowing the `on_data` message callbacks to
/// execute in parallel and append intermediate data to a state structure that can be utilized by
/// the `on_watermark` method to generate the final, complete data. Note that while the message
/// callbacks can execute in any order, the watermark callbacks execute sequentially and are
/// ordered by the timestamp order.
#[allow(unused_variables)]
pub trait ParallelOneInTwoOut<S, T, U, V, W>: Send + Sync
where
    S: AppendableStateT<W>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn run(
        &mut self,
        read_stream: &mut ReadStream<T>,
        left_write_stream: &mut WriteStream<U>,
        right_write_stream: &mut WriteStream<V>,
    ) {
    }

    fn destroy(&mut self) {}

    fn on_data(&self, ctx: &ParallelOneInTwoOutContext<S, U, V, W>, data: &T);

    fn on_watermark(&self, ctx: &mut ParallelOneInTwoOutContext<S, U, V, W>);
}

/// A context structure made available to the callbacks of a `ParallelOneInTwoOut` operator. The
/// context provides access to the current timestamp for which the callback is invoked along with
/// the state of the operator and the write streams to send the outputs on.
pub struct ParallelOneInTwoOutContext<'a, S, T, U, V>
where
    S: AppendableStateT<V>,
    T: Data + for<'b> Deserialize<'b>,
    U: Data + for<'b> Deserialize<'b>,
{
    timestamp: Timestamp,
    config: OperatorConfig,
    state: &'a S,
    left_write_stream: WriteStream<T>,
    right_write_stream: WriteStream<U>,
    phantom_v: PhantomData<V>,
}

impl<'a, S, T, U, V> ParallelOneInTwoOutContext<'a, S, T, U, V>
where
    S: AppendableStateT<V>,
    T: Data + for<'b> Deserialize<'b>,
    U: Data + for<'b> Deserialize<'b>,
{
    pub fn new(
        timestamp: Timestamp,
        config: OperatorConfig,
        state: &'a S,
        left_write_stream: WriteStream<T>,
        right_write_stream: WriteStream<U>,
    ) -> Self {
        Self {
            timestamp,
            config,
            state,
            left_write_stream,
            right_write_stream,
            phantom_v: PhantomData,
        }
    }

    /// Get the timestamp for which the callback was invoked.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Get the configuration of the operator.
    pub fn get_operator_config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Get the state attached to the operator.
    pub fn get_state(&self) -> &S {
        &self.state
    }

    /// Get the left write stream to send the output on.
    pub fn get_left_write_stream(&mut self) -> &mut WriteStream<T> {
        &mut self.left_write_stream
    }

    /// Get the right write stream to send the output on.
    pub fn get_right_write_stream(&mut self) -> &mut WriteStream<U> {
        &mut self.right_write_stream
    }
}

/**************************************************************************************************
 * OneInTwoOut: Receives data with type T, and enables message callbacks to have mutable          *
 * access to the operator state, but all callbacks are sequentialized.                            *
 *************************************************************************************************/

/// The `OneInTwoOut` trait must be implemented by operators that consume data from their input
/// read stream, and generate output on two output write streams. The operator can either choose to
/// consume data from its `read_stream` itself using the `run` method, or register for the
/// `on_data` and `on_watermark` callbacks.
///
/// The callbacks registered in this operator execute sequentially by timestamp order and are
/// allowed to mutate state in both the message and the watermark callbacks.
#[allow(unused_variables)]
pub trait OneInTwoOut<S, T, U, V>: Send + Sync
where
    S: StateT,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
    V: Data + for<'a> Deserialize<'a>,
{
    fn run(
        &mut self,
        read_stream: &mut ReadStream<T>,
        left_write_stream: &mut WriteStream<U>,
        right_write_stream: &mut WriteStream<V>,
    ) {
    }

    fn destroy(&mut self) {}

    fn on_data(&mut self, ctx: &mut OneInTwoOutContext<S, U, V>, data: &T);

    fn on_watermark(&mut self, ctx: &mut OneInTwoOutContext<S, U, V>);
}

/// A context structure made available to the callbacks of a `OneInTwoOut` operator. The context
/// provides access to the current timestamp for which the callback is invoked along with the
/// state of the operator and the write streams to send the outputs on.
pub struct OneInTwoOutContext<'a, S, T, U>
where
    S: StateT,
    T: Data + for<'b> Deserialize<'b>,
    U: Data + for<'b> Deserialize<'b>,
{
    timestamp: Timestamp,
    config: OperatorConfig,
    state: &'a mut S,
    left_write_stream: WriteStream<T>,
    right_write_stream: WriteStream<U>,
}

impl<'a, S, T, U> OneInTwoOutContext<'a, S, T, U>
where
    S: StateT,
    T: Data + for<'b> Deserialize<'b>,
    U: Data + for<'b> Deserialize<'b>,
{
    pub fn new(
        timestamp: Timestamp,
        config: OperatorConfig,
        state: &'a mut S,
        left_write_stream: WriteStream<T>,
        right_write_stream: WriteStream<U>,
    ) -> Self {
        Self {
            timestamp,
            config,
            state,
            left_write_stream,
            right_write_stream,
        }
    }

    /// Get the timestamp for which the callback was invoked.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Get the configuration of the operator.
    pub fn get_operator_config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Get the state attached to the operator.
    pub fn get_state(&mut self) -> &mut S {
        &mut self.state
    }

    /// Get the left write stream to send the output on.
    pub fn get_left_write_stream(&mut self) -> &mut WriteStream<T> {
        &mut self.left_write_stream
    }

    /// Get the right write stream to send the output on.
    pub fn get_right_write_stream(&mut self) -> &mut WriteStream<U> {
        &mut self.right_write_stream
    }
}

#[derive(Clone)]
pub struct OperatorConfig {
    /// A human-readable name for the [`Operator`] used in logging.
    pub name: Option<String>,
    /// A unique identifier for the [`Operator`].
    /// ERDOS sets this value when the dataflow graph executes.
    /// Currently the ID is inaccessible from the driver, but is set when the config
    /// is passed to the operator.
    pub id: OperatorId,
    /// Whether the [`Operator`] should automatically send
    /// [watermark messages](crate::dataflow::Message::Watermark) on all
    /// [`WriteStream`](crate::dataflow::WriteStream)s for
    /// [`Timestamp`](crate::dataflow::Timestamp) `t` upon receiving watermarks with
    /// [`Timestamp`](crate::dataflow::Timestamp) greater than `t` on all
    /// [`ReadStream`](crate::dataflow::ReadStream)s.
    /// Note that watermarks only flow after all watermark callbacks with timestamp
    /// less than `t` complete. Watermarks flow after [`Operator::run`] finishes
    /// running. Defaults to `true`.
    pub flow_watermarks: bool,
    /// The ID of the node on which the operator should run. Defaults to `0`.
    pub node_id: NodeId,
    /// Number of parallel tasks which process callbacks.
    /// A higher number may result in more parallelism; however this may be limited
    /// by dependencies on [`State`](crate::dataflow::State) and timestamps.
    pub num_event_runners: usize,
}

impl OperatorConfig {
    pub fn new() -> Self {
        Self {
            id: OperatorId::nil(),
            name: None,
            flow_watermarks: true,
            node_id: 0,
            num_event_runners: 1,
        }
    }

    /// Set the [`Operator`]'s name.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set whether the [`Operator`] should flow watermarks.
    pub fn flow_watermarks(mut self, flow_watermarks: bool) -> Self {
        self.flow_watermarks = flow_watermarks;
        self
    }

    /// Set the node on which the [`Operator`] runs.
    pub fn node(mut self, node_id: NodeId) -> Self {
        self.node_id = node_id;
        self
    }

    /// Sets the maximum number of callbacks the operator can process in parallel
    /// at a time. Defaults to 1.
    pub fn num_event_runners(mut self, num_event_runners: usize) -> Self {
        assert!(
            num_event_runners > 0,
            "Operator must have at least 1 thread."
        );
        self.num_event_runners = num_event_runners;
        self
    }

    /// Returns the name operator. If the name is not set,
    /// returns the ID of the operator.
    pub fn get_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| format!("{}", self.id))
    }
}
