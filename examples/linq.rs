use std::{thread, time::Duration};

use erdos::{
    dataflow::{
        context::SinkContext,
        operator::{Sink, Source},
        operators::{Filter, Map, Split},
        state::TimeVersionedState,
        stream::{WriteStream, WriteStreamT},
        Message, OperatorConfig, Timestamp,
    },
    node::Node,
    Configuration,
};

struct SourceOperator {}

impl SourceOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Source<usize> for SourceOperator {
    fn run(&mut self, _operator_config: &OperatorConfig, write_stream: &mut WriteStream<usize>) {
        tracing::info!("Running Source Operator");
        for t in 0..10 {
            let timestamp = Timestamp::Time(vec![t as u64]);
            write_stream
                .send(Message::new_message(timestamp.clone(), t))
                .unwrap();
            write_stream
                .send(Message::new_watermark(timestamp))
                .unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn destroy(&mut self) {
        tracing::info!("Destroying Source Operator");
    }
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<TimeVersionedState<usize>, usize> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>, data: &usize) {
        let timestamp = ctx.get_timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received {}",
            ctx.get_operator_config().get_name(),
            timestamp,
            data,
        );

        // Increment the message count.
        *ctx.get_current_state().unwrap() += 1;
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>) {
        let timestamp = ctx.get_timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received {} data messages.",
            ctx.get_operator_config().get_name(),
            timestamp,
            ctx.get_current_state().unwrap(),
        );
    }
}

fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let source_config = OperatorConfig::new().name("SourceOperator");
    let source_stream = erdos::connect_source(SourceOperator::new, source_config);

    let (split_stream_less_50, split_stream_greater_50) = source_stream
        .map(|x: &usize| -> usize { 2 * x })
        .filter(|x: &usize| -> bool { x > &10 })
        .split(|x: &usize| -> bool { x < &50 });

    let left_sink_config = OperatorConfig::new().name("LeftSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        left_sink_config,
        &split_stream_less_50,
    );

    let right_sink_config = OperatorConfig::new().name("RightSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        right_sink_config,
        &split_stream_greater_50,
    );

    node.run();
}
