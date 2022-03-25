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
/// The following table provides an example of how the [`TimestampJoin`] processes two streams:
/// | Timestamp | Data sent on left stream | Data sent on right stream |
/// |-----------|--------------------------|---------------------------|
/// | asdf      |                q         |                           |
/// | 1         | 2                        | 3                         |
pub struct TimestampJoin {}

impl TimestampJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl<T, U> TwoInOneOut<TimeVersionedState<(Vec<T>, Vec<U>)>, T, U, (T, U)> for TimestampJoin
where
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn on_left_data(
        &mut self,
        ctx: &mut TwoInOneOutContext<TimeVersionedState<(Vec<T>, Vec<U>)>, (T, U)>,
        data: &T,
    ) {
        let timestamp = ctx.get_timestamp().clone();
        let (left_items, right_items) = ctx.get_current_state().unwrap();
        left_items.push(data.clone());

        let messages: Vec<_> = right_items
            .iter()
            .cloned()
            .map(|right_item| Message::new_message(timestamp.clone(), (data.clone(), right_item)))
            .collect();
        for msg in messages {
            ctx.get_write_stream().send(msg).unwrap();
        }
    }

    fn on_right_data(
        &mut self,
        ctx: &mut TwoInOneOutContext<TimeVersionedState<(Vec<T>, Vec<U>)>, (T, U)>,
        data: &U,
    ) {
        let timestamp = ctx.get_timestamp().clone();
        let (left_items, right_items) = ctx.get_current_state().unwrap();
        right_items.push(data.clone());

        let messages: Vec<_> = left_items
            .iter()
            .cloned()
            .map(|left_item| Message::new_message(timestamp.clone(), (left_item, data.clone())))
            .collect();
        for msg in messages {
            ctx.get_write_stream().send(msg).unwrap();
        }
    }

    fn on_watermark(
        &mut self,
        ctx: &mut TwoInOneOutContext<TimeVersionedState<(Vec<T>, Vec<U>)>, (T, U)>,
    ) {
        let timestamp = ctx.get_timestamp().clone();
        ctx.get_state_mut().evict_until(&timestamp);
    }
}

/// TODO: document
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
    fn timestamp_join(&self, other: &dyn Stream<U>) -> OperatorStream<(T, U)> {
        let name = format!("TimestampJoinOp_{}_{}", self.name(), other.name());
        crate::connect_two_in_one_out(
            TimestampJoin::new,
            TimeVersionedState::new,
            OperatorConfig::new().name(&name),
            self,
            other,
        )
    }
}
