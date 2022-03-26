use std::{thread, time::Duration};

use erdos::{
    dataflow::{
        context::SinkContext,
        operator::{Sink, Source},
        operators::{Filter, Join, Map, Split},
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
    // Streams data 0, 1, 2, ..., 9 with timestamps 0, 1, 2, ..., 9.
    let source_stream = erdos::connect_source(SourceOperator::new, source_config);

    // Given x, generates a sequence of messages 0, ..., x for the current timestamp.
    let sequence = source_stream.flat_map(|x| (1..=*x));
    // Finds the factors of x using the generated sequence.
    let factors = source_stream
        .timestamp_join(&sequence)
        .filter(|&(x, d)| x % d == 0)
        .map(|&(_, d)| d);

    // Split into streams of even factors and odd factors.
    let (evens, odds) = factors.split(|x| x % 2 == 0);

    // Print received even messages.
    let evens_sink_config = OperatorConfig::new().name("EvensSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        evens_sink_config,
        &evens,
    );

    // Print received odd messages.
    let odds_sink_config = OperatorConfig::new().name("OddsSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        odds_sink_config,
        &odds,
    );

    node.run();
}
