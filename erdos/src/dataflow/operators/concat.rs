use serde::Deserialize;

use crate::{
    dataflow::{
        context::TwoInOneOutContext,
        operator::TwoInOneOut,
        stream::{OperatorStream, WriteStreamT},
        Data, Message, Stream,
    },
    OperatorConfig,
};

/// Merges the contents of two streams.
///
/// Data messages are sent on the merged stream in order of arrival.
/// A watermark is sent when the minimum watermark received across both streams advances.
/// In other words, when `min(left_watermark_timestamp, right_watermark_timestamp)` increases,
/// the operator sends a watermark with an equivalent timestamp.
///
/// ```
/// # use erdos::dataflow::{stream::{IngestStream, Stream}, operator::OperatorConfig, operators::ConcatOperator};
/// # let left_stream: IngestStream<usize> = IngestStream::new();
/// # let right_stream: IngestStream<usize> = IngestStream::new();
/// #
/// let merged_stream = erdos::connect_two_in_one_out(
///     ConcatOperator::new,
///     || {},
///     OperatorConfig::new().name("ConcatOperator"),
///     &left_stream,
///     &right_stream,
/// );
/// ```
#[derive(Default)]
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
        let msg = Message::new_message(ctx.timestamp().clone(), data.clone());
        ctx.write_stream().send(msg).unwrap();
    }

    fn on_right_data(&mut self, ctx: &mut TwoInOneOutContext<(), D>, data: &D) {
        let msg = Message::new_message(ctx.timestamp().clone(), data.clone());
        ctx.write_stream().send(msg).unwrap();
    }

    fn on_watermark(&mut self, _ctx: &mut TwoInOneOutContext<(), D>) {}
}

/// Extension trait for merging the contents of two streams.
///
/// Names the [`ConcatOperator`] using the names of the two merged streams.
///
/// # Example
/// ```
/// # use erdos::dataflow::{stream::{IngestStream, Stream}, operator::OperatorConfig, operators::Concat};
/// # let left_stream: IngestStream<usize> = IngestStream::new();
/// # let right_stream: IngestStream<usize> = IngestStream::new();
/// #
/// let merged_stream = left_stream.concat(&right_stream);
/// ```
pub trait Concat<D>
where
    D: Data + for<'a> Deserialize<'a>,
{
    fn concat(&self, other: &dyn Stream<D>) -> OperatorStream<D>;
}

impl<S, D> Concat<D> for S
where
    S: Stream<D>,
    D: Data + for<'a> Deserialize<'a>,
{
    fn concat(&self, other: &dyn Stream<D>) -> OperatorStream<D> {
        let name = format!("ConcatOp_{}_{}", self.name(), other.name());
        crate::connect_two_in_one_out(
            ConcatOperator::new,
            || {},
            OperatorConfig::new().name(&name),
            self,
            other,
        )
    }
}
