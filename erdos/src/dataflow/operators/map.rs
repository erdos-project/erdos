use std::sync::Arc;

use serde::Deserialize;

use crate::dataflow::{
    context::OneInOneOutContext,
    message::Message,
    operator::{OneInOneOut, OperatorConfig},
    stream::{Stream, WriteStreamT},
    Data,
};

/// Maps an incoming stream of type D to a stream of type `I::Item` using the provided
/// function.
///
/// # Example
/// The below example shows how to use a [`FlatMapOperator`] to double an incoming stream of usize
/// messages, and return them.
///
/// ```F
/// # use erdos::dataflow::{stream::IngestStream, operator::{OperatorConfig}, operators::{FlatMapOperator}};
/// # let source_stream = IngestStream::new();
/// let map_stream = erdos::connect_one_in_one_out(
///     || -> FlatMapOperator<usize, usize, _> { MapOperator::new(|a: &usize| -> Vec<usize> { vec![2 * a] }) },
///     || {},
///     OperatorConfig::new().name("FlatMapOperator"),
///     &source_stream,
/// );
/// ```
pub struct FlatMapOperator<D, I>
where
    D: Data + for<'a> Deserialize<'a>,
    I: IntoIterator,
    I::Item: Data + for<'a> Deserialize<'a>,
{
    flat_map_fn: Arc<dyn Fn(&D) -> I + Send + Sync>,
}

impl<D, I> FlatMapOperator<D, I>
where
    D: Data + for<'a> Deserialize<'a>,
    I: IntoIterator,
    I::Item: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(flat_map_fn: F) -> Self
    where
        F: 'static + Fn(&D) -> I + Send + Sync,
    {
        Self {
            flat_map_fn: Arc::new(flat_map_fn),
        }
    }
}

impl<D, I> OneInOneOut<(), D, I::Item> for FlatMapOperator<D, I>
where
    D: Data + for<'a> Deserialize<'a>,
    I: IntoIterator,
    I::Item: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut OneInOneOutContext<(), I::Item>, data: &D) {
        tracing::info!("on data");
        for item in (self.flat_map_fn)(data).into_iter() {
            tracing::debug!(
                "{} @ {:?}: received {:?} and sending {:?}",
                ctx.get_operator_config().get_name(),
                ctx.get_timestamp(),
                data,
                item,
            );

            let timestamp = ctx.get_timestamp().clone();
            let msg = Message::new_message(timestamp, item);
            ctx.get_write_stream().send(msg).unwrap();
        }
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<(), I::Item>) {
        tracing::info!("{}: got watermark", _ctx.get_operator_config().get_name());
    }
}

// Extension Trait for MapOperator
pub trait Map<D, D2>
where
    D: Data + for<'a> Deserialize<'a>,
    D2: Data + for<'a> Deserialize<'a>,
{
    fn map<F>(&self, map_fn: F) -> Stream<D2>
    where
        F: 'static + Fn(&D) -> D2 + Send + Sync + Clone;

    fn flat_map<F, I>(&self, flat_map_fn: F) -> Stream<D2>
    where
        F: 'static + Fn(&D) -> I + Send + Sync + Clone,
        I: 'static + IntoIterator<Item = D2>;
}

impl<D1, D2> Map<D1, D2> for Stream<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
    D2: Data + for<'a> Deserialize<'a>,
{
    fn map<F>(&self, map_fn: F) -> Stream<D2>
    where
        F: 'static + Fn(&D1) -> D2 + Send + Sync + Clone,
    {
        let op_name = format!("MapOp_{}", self.id());

        crate::connect_one_in_one_out(
            move || -> FlatMapOperator<D1, _> {
                let map_fn = map_fn.clone();
                FlatMapOperator::new(move |x| std::iter::once(map_fn(x)))
            },
            || {},
            OperatorConfig::new().name(&op_name),
            self,
        )
    }

    fn flat_map<F, I>(&self, flat_map_fn: F) -> Stream<D2>
    where
        F: 'static + Fn(&D1) -> I + Send + Sync + Clone,
        I: 'static + IntoIterator<Item = D2>,
    {
        let op_name = format!("MapOp_{}", self.id());

        crate::connect_one_in_one_out(
            move || -> FlatMapOperator<D1, _> { FlatMapOperator::new(flat_map_fn.clone()) },
            || {},
            OperatorConfig::new().name(&op_name),
            self,
        )
    }
}
