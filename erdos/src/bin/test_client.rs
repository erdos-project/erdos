use erdos::{dataflow::{operator::Sink, state::TimeVersionedState, context::SinkContext, graph::AbstractGraph}, node::Client};
use futures::stream::StreamExt;

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<TimeVersionedState<usize>, usize> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>, data: &usize) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received {}",
            ctx.operator_config().get_name(),
            timestamp,
            data
        );
        *ctx.current_state().unwrap() += 1;
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received {} data messages.",
            ctx.operator_config().get_name(),
            timestamp,
            ctx.current_state().unwrap(),
        );
    }
}

#[tokio::main]
async fn main() {
    let addr = "0.0.0.0:4444".parse().unwrap();
    let client = Client::new(addr);

    client.submit(&AbstractGraph::new());

    while true {}
}