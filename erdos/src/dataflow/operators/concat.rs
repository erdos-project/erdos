use serde::Deserialize;

use crate::dataflow::{
    context::TwoInOneOutContext, operator::TwoInOneOut, stream::WriteStreamT, Data, Message,
};

/// Merges the contents of two streams.
///
/// Data messages are sent on the merged stream in order of arrival.
/// A watermark is sent when the minimum watermark received across both streams advances.
/// In other words, when `min(left_watermark_timestamp, right_watermark_timestamp)` increases,
/// the operator sends a watermark with an equivalent timestamp.
///
///
pub struct ConcatOperator {}

impl ConcatOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl<D: Data> TwoInOneOut<(), D, D, D> for ConcatOperator
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn on_left_data(&mut self, ctx: &mut TwoInOneOutContext<(), D>, data: &D) {
        let msg = Message::new_message(ctx.get_timestamp().clone(), data.clone());
        ctx.get_write_stream().send(msg).unwrap();
    }

    fn on_right_data(&mut self, ctx: &mut TwoInOneOutContext<(), D>, data: &D) {
        let msg = Message::new_message(ctx.get_timestamp().clone(), data.clone());
        ctx.get_write_stream().send(msg).unwrap();
    }

    fn on_watermark(&mut self, ctx: &mut TwoInOneOutContext<(), D>) {}
}

#[cfg(test)]
mod test {
    #[test]
    fn test_concat() {}
}
