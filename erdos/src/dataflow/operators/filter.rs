use std::sync::Arc;

use serde::Deserialize;

use crate::dataflow::{
    context::OneInOneOutContext,
    message::Message,
    operator::{OneInOneOut, OperatorConfig},
    stream::{OperatorStream, Stream, WriteStreamT},
    Data,
};

/// Filters an incoming stream of type D, retaining messages in the stream that
/// the provided condition function evaluates to true when applied.
///
/// # Example
/// The below example shows how to use a FilterOperator to keep only messages > 10 in an incoming
/// stream of usize messages, and send them.
///
/// ```
/// # use erdos::dataflow::{stream::IngestStream, operator::{OperatorConfig}, operators::{FilterOperator}};
/// # let source_stream = IngestStream::new();
/// // Add the mapping function as an argument to the operator via the OperatorConfig.
/// let filter_config = OperatorConfig::new().name("FilterOperator");
/// let filter_stream = erdos::connect_one_in_one_out(
///     || -> FilterOperator<usize> { FilterOperator::new(|a: &usize| -> bool { a > &10 }) },
///     || {},
///     filter_config,
///     &source_stream,
/// );
/// ```
pub struct FilterOperator<D>
where
    D: Data + for<'a> Deserialize<'a>,
{
    filter_function: Arc<dyn Fn(&D) -> bool + Send + Sync>,
}

impl<D> FilterOperator<D>
where
    D: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(filter_function: F) -> Self
    where
        F: 'static + Fn(&D) -> bool + Send + Sync,
    {
        Self {
            filter_function: Arc::new(filter_function),
        }
    }
}

impl<D> OneInOneOut<(), D, D> for FilterOperator<D>
where
    D: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut OneInOneOutContext<(), D>, data: &D) {
        let timestamp = ctx.timestamp().clone();
        if (self.filter_function)(data) {
            ctx.write_stream()
                .send(Message::new_message(timestamp, data.clone()))
                .unwrap();
            tracing::debug!(
                "{} @ {:?}: received {:?} and sent it",
                ctx.operator_config().get_name(),
                ctx.timestamp(),
                data,
            );
        }
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<(), D>) {}
}

// Extension trait for FilterOperator
pub trait Filter<D>
where
    D: Data + for<'a> Deserialize<'a>,
{
    fn filter<F>(&self, filter_fn: F) -> OperatorStream<D>
    where
        F: 'static + Fn(&D) -> bool + Send + Sync + Clone;
}

impl<S, D> Filter<D> for S
where
    S: Stream<D>,
    D: Data + for<'a> Deserialize<'a>,
{
    fn filter<F>(&self, filter_fn: F) -> OperatorStream<D>
    where
        F: 'static + Fn(&D) -> bool + Send + Sync + Clone,
    {
        let op_name = format!("FilterOp_{}", self.id());

        let write_stream = OperatorStream::new(Arc::clone(&self.get_graph()));
        self.get_graph().lock().unwrap().connect_one_in_one_out(
            move || -> FilterOperator<D> { FilterOperator::new(filter_fn.clone()) },
            || {},
            OperatorConfig::new().name(&op_name),
            self,
            write_stream.clone(),
        );
        write_stream
    }
}
