use std::sync::Arc;

use serde::Deserialize;

use crate::dataflow::{
    context::OneInTwoOutContext,
    message::Message,
    operator::{OneInTwoOut, OperatorConfig},
    stream::{Stream, WriteStreamT},
    Data,
};

/// Splits an incoming stream of type D1 into two different streams of type D1 using the provided
/// condition function. When evaluated to true, sends messages to left stream, and right stream
/// otherwise.
///
/// # Example
/// The below example shows how to use a SplitOperator to split an incoming stream of usize messages into two different streams
/// one with messages > 10 (left stream) and one with messages <= 10 (right stream), and send them.
///
/// ```
/// // Add the mapping function as an argument to the operator via the OperatorConfig.
/// let split_config = OperatorConfig::new().name("SplitOperator");
/// let (left_stream, right_stream) = erdos::connect_one_in_one_out(
///     || -> SplitOperator<usize> { SplitOperator::new(|a: &usize| -> bool { a > &10 }) },
///     || {},
///     split_config,
///     &source_stream,
/// );
/// ```
pub struct SplitOperator<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    split_function: Arc<dyn Fn(&D1) -> bool + Send + Sync>,
}

impl<D1> SplitOperator<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(split_function: F) -> Self
    where
        F: 'static + Fn(&D1) -> bool + Send + Sync,
    {
        Self {
            split_function: Arc::new(split_function),
        }
    }
}

impl<D1> OneInTwoOut<(), D1, D1, D1> for SplitOperator<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut OneInTwoOutContext<(), D1, D1>, data: &D1) {
        let timestamp = ctx.get_timestamp().clone();
        let mut stream_side: &str = "left";

        let write_stream = if (self.split_function)(data) {
            ctx.get_left_write_stream()
        } else {
            stream_side = "right";
            ctx.get_right_write_stream()
        };

        write_stream
            .send(Message::new_message(timestamp, data.clone()))
            .unwrap();
        tracing::debug!(
            "{} @ {:?}: received {:?} and sent to {} stream",
            ctx.get_operator_config().get_name(),
            ctx.get_timestamp(),
            data,
            stream_side
        );
    }

    fn on_watermark(&mut self, _ctx: &mut OneInTwoOutContext<(), D1, D1>) {}
}

// Extension trait for SplitOperator
pub trait Split<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    fn split<F: 'static + Fn(&D1) -> bool + Send + Sync + Clone>(
        &self,
        split_fn: F,
    ) -> (Stream<D1>, Stream<D1>);
}

impl<D1> Split<D1> for Stream<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    fn split<F: 'static + Fn(&D1) -> bool + Send + Sync + Clone>(
        &self,
        split_fn: F,
    ) -> (Stream<D1>, Stream<D1>) {
        let op_name = format!("SplitOp_{}", self.id());

        crate::connect_one_in_two_out(
            move || -> SplitOperator<D1> { SplitOperator::new(split_fn.clone()) },
            || {},
            OperatorConfig::new().name(&op_name),
            self,
        )
    }
}
