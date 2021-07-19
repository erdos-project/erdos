use std::{marker::PhantomData, sync::Arc};

use serde::Deserialize;

use crate::dataflow::{
    deadlines::DeadlineT, operator::OperatorConfig, stream::StreamId, AppendableStateT, Data,
    StateT, Timestamp, WriteStream,
};

/*************************************************************************************************
 * SetupContext: Provided to an operator's `setup` method, and allows the operators to register  *
 * deadlines.                                                                                    *
 ************************************************************************************************/

/// A `SetupContext` is made available to an operator's `setup` method, and allows the operators to
/// register deadlines for events along with their corresponding handlers.
pub struct SetupContext {
    deadlines: Vec<Arc<dyn DeadlineT>>,
    // TODO (Sukrit): Can we provide a better interface than ReadStream and WriteStream IDs?
    read_stream_ids: Vec<StreamId>,
    write_stream_ids: Vec<StreamId>,
}

impl SetupContext {
    pub fn new(read_stream_ids: Vec<StreamId>, write_stream_ids: Vec<StreamId>) -> Self {
        Self {
            deadlines: Vec::new(),
            read_stream_ids,
            write_stream_ids,
        }
    }

    /// Register a deadline with the system.
    pub fn add_deadline<S: DeadlineT + 'static>(&mut self, deadline: S) {
        self.deadlines.push(Arc::new(deadline));
    }

    /// Get the deadlines registered in this context.
    pub(crate) fn get_deadlines(&mut self) -> &mut Vec<Arc<dyn DeadlineT>> {
        &mut self.deadlines
    }

    /// Get the identifiers of the read streams of this operator.
    pub fn get_read_stream_ids(&self) -> &Vec<StreamId> {
        &self.read_stream_ids
    }

    /// Get the identifiers of the write streams of this operator.
    pub fn get_write_stream_ids(&self) -> &Vec<StreamId> {
        &self.write_stream_ids
    }
}

/*************************************************************************************************
 * ParallelSinkContext: Provides access to the state registered with a ParallelSink operator in  *
 * the message and watermark callbacks.                                                          *
 ************************************************************************************************/

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
 * SinkContext: Provides access to the state registered with a Sink operator in the message and  *
 * watermark callbacks.                                                                          *
 ************************************************************************************************/

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

/************************************************************************************************
 * ParallelOneInOneOutContext: Provides access to the state and the write stream registered     *
 * with a ParallelOneInOneOut operator in the message and watermark callbacks.                  *
 ************************************************************************************************/

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

/************************************************************************************************
 * OneInOneOutContext: Provides access to the state and the write stream registered with a      *
 * OneInOneOut operator in the message and watermark callbacks.                                 *
 ************************************************************************************************/

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

/************************************************************************************************
 * ParallelTwoInOneOutContext: Provides access to the state and the write stream registered     *
 * with a ParallelTwoInOneOut operator in the message and watermark callbacks.                  *
 ************************************************************************************************/

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

/************************************************************************************************
 * TwoInOneOutContext: Provides access to the state and the write stream registered with a      *
 * TwoInOneOut operator in the message and watermark callbacks.                                 *
 ************************************************************************************************/

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

/************************************************************************************************
 * ParallelOneInTwoOutContext: Provides access to the state and the write streams registered    *
 * with a ParallelOneInTwoOut operator in the message and watermark callbacks.                  *
 ************************************************************************************************/

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

/************************************************************************************************
 * OneInTwoOutContext: Provides access to the state and the write streams registered with a     *
 * OneInTwoOut operator in the message and watermark callbacks.                                 *
 ************************************************************************************************/

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
