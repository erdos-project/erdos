use std::sync::Arc;

use serde::Deserialize;

use crate::dataflow::{
    context::TwoInOneOutContext,
    message::Message,
    operator::{OperatorConfig, TwoInOneOut},
    state::TimeVersionedState,
    stream::{OperatorStream, Stream, WriteStreamT},
    Data,
};

/// Joins messages with matching timestamps from two different streams.
///
/// The following table provides an example of how the [`TimestampJoinOperator`] processes data
/// from two streams:
///
/// | Timestamp | Left input | Right input | [`TimestampJoinOperator`] output           |
/// |-----------|------------|-------------|--------------------------------------------|
/// | 1         | a <br> b   | 1 <br> 2    | (a, 1) <br> (a, 2) <br> (b, 1) <br> (b, 2) |
/// | 2         | c          |             |                                            |
/// | 3         |            | 3           |                                            |
/// | 4         | d          | 4           | (d, 4)                                     |
///
/// # Example
/// The following example shows how to use a [`TimestampJoinOperator`] to join two streams.
///
/// ```
/// # use erdos::dataflow::{
/// #     stream::IngressStream,
/// #     operator::OperatorConfig,
/// #     operators::TimestampJoinOperator,
/// #     state::TimeVersionedState,
/// #     Graph,
/// # };
/// # let graph = Graph::new();
/// # let left_stream: IngressStream<String> = graph.add_ingress("LeftIngressStream");
/// # let right_stream: IngressStream<usize> = graph.add_ingress("RightIngressStream");
/// #
/// // Joins two streams of types String and usize
/// let joined_stream = graph.connect_two_in_one_out(
///     TimestampJoinOperator::new,
///     TimeVersionedState::new,
///     OperatorConfig::new().name("TimestampJoinOperator"),
///     &left_stream,
///     &right_stream,
/// );
/// ```
#[derive(Default)]
pub struct TimestampJoinOperator {}

impl TimestampJoinOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl<T, U> TwoInOneOut<TimeVersionedState<(Vec<T>, Vec<U>)>, T, U, (T, U)> for TimestampJoinOperator
where
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn on_left_data(
        &mut self,
        ctx: &mut TwoInOneOutContext<TimeVersionedState<(Vec<T>, Vec<U>)>, (T, U)>,
        data: &T,
    ) {
        let (left_items, right_items) = ctx.current_state().unwrap();
        left_items.push(data.clone());

        // Can't iterate through right_msgs and send messages because this results in a compiler
        // error due to 2 mutable references to ctx.
        let num_right_items = right_items.len();
        for i in 0..num_right_items {
            let right_item = ctx.current_state().unwrap().1[i].clone();
            let msg = Message::new_message(ctx.timestamp().clone(), (data.clone(), right_item));
            ctx.write_stream().send(msg).unwrap();
        }
    }

    fn on_right_data(
        &mut self,
        ctx: &mut TwoInOneOutContext<TimeVersionedState<(Vec<T>, Vec<U>)>, (T, U)>,
        data: &U,
    ) {
        let (left_items, right_items) = ctx.current_state().unwrap();
        right_items.push(data.clone());

        // Can't iterate through left_items and send messages because this results in a compiler
        // error due to 2 mutable references to ctx.
        let num_left_items = left_items.len();
        for i in 0..num_left_items {
            let left_item = ctx.current_state().unwrap().0[i].clone();
            let msg = Message::new_message(ctx.timestamp().clone(), (left_item, data.clone()));
            ctx.write_stream().send(msg).unwrap();
        }
    }

    fn on_watermark(
        &mut self,
        ctx: &mut TwoInOneOutContext<TimeVersionedState<(Vec<T>, Vec<U>)>, (T, U)>,
    ) {
        let timestamp = ctx.timestamp().clone();
        ctx.state_mut().evict_until(&timestamp);
    }
}

/// Extension trait for joining pairs of streams.
///
/// Names the operators using the names of the incoming streams.
pub trait Join<T, U>
where
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn timestamp_join(&self, other: &dyn Stream<U>) -> OperatorStream<(T, U)>;
}

impl<S, T, U> Join<T, U> for S
where
    S: Stream<T>,
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    /// Joins messages with matching timestamps from two different streams using a
    /// [`TimestampJoinOperator`].
    ///
    /// # Example
    ///
    /// ```
    /// # use erdos::dataflow::{Graph, stream::IngressStream, operators::Join};
    /// # let graph = Graph::new();
    /// # let left_stream: IngressStream<String> = graph.add_ingress("LeftIngressStream");
    /// # let right_stream: IngressStream<usize> = graph.add_ingress("RightIngressStream");
    /// #
    /// let joined_stream = left_stream.timestamp_join(&right_stream);
    /// ```
    fn timestamp_join(&self, other: &dyn Stream<U>) -> OperatorStream<(T, U)> {
        let op_name = format!("TimestampJoinOp_{}_{}", self.name(), other.name());
        let write_stream = OperatorStream::new(
            &format!("{}-write-stream", op_name),
            Arc::clone(&self.graph()),
        );

        self.graph().lock().unwrap().connect_two_in_one_out(
            TimestampJoinOperator::new,
            TimeVersionedState::new,
            OperatorConfig::new().name(&op_name),
            self,
            other,
            &write_stream,
        );
        write_stream
    }
}
