use crate::dataflow::{
    context::OneInOneOutContext, message::Message, operator::OneInOneOut, stream::WriteStreamT,
    Data,
};
use serde::Deserialize;
use std::sync::Arc;

/// Maps an incoming stream of type D1 to a stream of type D2 using the provided
/// function.
///
/// # Example
/// The below example shows how to use a MapOperator to double an incoming stream of usize messages,
/// and return them.
///
/// ```
/// # Add the mapping function as an argument to the operator via the OperatorConfig.
/// let map_config = OperatorConfig::new().name("MapOperator");
/// let map_stream = erdos::connect_one_in_one_out(
///     || -> MapOperator<usize, usize> { MapOperator::new(|a: &usize| -> usize { 2 * a }) },
///     || {},
///     map_config,
///     &source_stream,
/// );
/// ```
pub struct MapOperator<D1, D2>
where
    D1: Data + for<'a> Deserialize<'a>,
    D2: Data + for<'a> Deserialize<'a>,
{
    map_function: Arc<dyn Fn(&D1) -> D2 + Send + Sync>,
}

impl<D1, D2> MapOperator<D1, D2>
where
    D1: Data + for<'a> Deserialize<'a>,
    D2: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(map_function: F) -> Self
    where
        F: 'static + Fn(&D1) -> D2 + Send + Sync,
    {
        Self {
            map_function: Arc::new(map_function),
        }
    }
}

impl<D1, D2> OneInOneOut<(), D1, D2> for MapOperator<D1, D2>
where
    D1: Data + for<'a> Deserialize<'a>,
    D2: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut OneInOneOutContext<(), D2>, data: &D1) {
        let timestamp = ctx.get_timestamp().clone();
        ctx.get_write_stream()
            .send(Message::new_message(timestamp, (self.map_function)(data)))
            .unwrap();
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "{} @ {:?}: received {:?} and sent {:?}",
            ctx.get_operator_config().get_name(),
            ctx.get_timestamp(),
            data,
            (self.map_function)(data)
        );
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<(), D2>) {}
}
