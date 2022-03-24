use std::{cmp::Reverse, collections::HashMap};

use serde::Deserialize;

use crate::dataflow::{
    context::TwoInOneOutContext,
    message::Message,
    operator::{OperatorConfig, TwoInOneOut},
    state::TimeVersionedState,
    stream::{OperatorStream, Stream, WriteStreamT},
    Data, Timestamp,
};

/// Joins messages with equivalent timestamps from two different streams.
/// TODO: more documentation
pub struct TimestampJoin<T: Data, U: Data> {
    /// Lists of received data, grouped by timestamp.
    items: HashMap<Timestamp, (Vec<T>, Vec<U>)>,
}

impl<T: Data, U: Data> TimestampJoin<T, U> {
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }
}

impl<T, U> TwoInOneOut<(), T, U, (T, U)> for TimestampJoin<T, U>
where
    T: Data + for<'a> Deserialize<'a>,
    U: Data + for<'a> Deserialize<'a>,
{
    fn on_left_data(&mut self, ctx: &mut TwoInOneOutContext<(), (T, U)>, data: &T) {
        let (left_items, right_items) = self.items.entry(ctx.get_timestamp().clone()).or_default();
        left_items.push(data.clone());

        for right_item in right_items.iter().cloned() {
            let msg = Message::new_message(ctx.get_timestamp().clone(), (data.clone(), right_item));
            ctx.get_write_stream().send(msg).unwrap();
        }
    }

    fn on_right_data(&mut self, ctx: &mut TwoInOneOutContext<(), (T, U)>, data: &U) {
        let (left_items, right_items) = self.items.entry(ctx.get_timestamp().clone()).or_default();
        right_items.push(data.clone());

        for left_item in left_items.iter().cloned() {
            let msg = Message::new_message(ctx.get_timestamp().clone(), (left_item, data.clone()));
            ctx.get_write_stream().send(msg).unwrap();
        }
    }

    fn on_watermark(&mut self, ctx: &mut TwoInOneOutContext<(), (T, U)>) {
        // Garbage-collect stale items.
        self.items.retain(|k, _| k > ctx.get_timestamp());
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
            || {},
            OperatorConfig::new().name(&name),
            self,
            other,
        )
    }
}
