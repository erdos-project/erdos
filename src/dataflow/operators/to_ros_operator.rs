use crate::dataflow::{
    context::SinkContext, operator::Sink,
    Data,
};
use serde::Deserialize;
use std::sync::Arc;

pub struct ToRosOperator<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    output_function: Arc<dyn Fn(&D1) -> bool + Send + Sync>,
}

impl<D1> ToRosOperator<D1> 
where
    D1: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(output_function: F) -> Self 
    where
        F: 'static + Fn(&D1) -> bool + Send + Sync, 
    {
        Self {
            output_function: Arc::new(output_function),
        }
    }
}

impl<D1> Sink<(), D1> for ToRosOperator<D1>
where
    D1: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut SinkContext<()>, data: &D1) {
        let timestamp = ctx.get_timestamp().clone();
        slog::info!(
            crate::TERMINAL_LOGGER,
            "ToRosOperator @ {:?}: Received {:?}",
            timestamp,
            data
        );
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<()>) {}
}