use crate::dataflow::{
    context::OneInOneOutContext, message::Message, operator::OneInOneOut, stream::WriteStreamT,
    Data,
};
use serde::Deserialize;
use std::sync::Arc;

/// Filters an incoming stream of type D1, retaining messages in the stream that
/// the provided condition function evaluates to true when applied.
///
/// # Example
/// The below example shows how to use a FilterOperator to keep only messages > 10 in an incoming stream of usize messages,
/// and send them.
///
/// ```
/// // Add the mapping function as an argument to the operator via the OperatorConfig.
/// let filter_config = OperatorConfig::new().name("FilterOperator");
/// let filter_stream = erdos::connect_one_in_one_out(
///     || -> FilterOperator<usize> { FilterOperator::new(|a: &usize| -> bool { a > &10 }) },
///     || {},
///     filter_config,
///     &source_stream,
/// );
/// ```
pub struct FilterOperator<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    filter_function: Arc<dyn Fn(&D1) -> bool + Send + Sync>,
}

impl<D1> FilterOperator<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(filter_function: F) -> Self
    where
        F: 'static + Fn(&D1) -> bool + Send + Sync,
    {
        Self {
            filter_function: Arc::new(filter_function),
        }
    }
}

impl<D1> OneInOneOut<(), D1, D1> for FilterOperator<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut OneInOneOutContext<(), D1>, data: &D1) {
        let timestamp = ctx.get_timestamp().clone();
        if (self.filter_function)(data) {
            ctx.get_write_stream()
                .send(Message::new_message(timestamp, data.clone()))
                .unwrap();
            slog::debug!(
                crate::TERMINAL_LOGGER,
                "{} @ {:?}: received {:?} and sent it",
                ctx.get_operator_config().get_name(),
                ctx.get_timestamp(),
                data,
            );
        }
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<(), D1>) {}
}
