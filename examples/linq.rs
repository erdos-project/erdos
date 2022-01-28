use std::{collections::HashMap, thread, time::Duration};

use erdos::{
    dataflow::{
        context::SinkContext,
        operator::{Sink, Source},
        operators::{Filter, Map, Split},
        stream::{WriteStream, WriteStreamT},
        Message, OperatorConfig, StateT, Timestamp,
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

impl Source<(), usize> for SourceOperator {
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

struct SinkOperatorState {
    message_counter: HashMap<Timestamp, usize>,
    current_timestamp: Timestamp,
}

impl SinkOperatorState {
    fn new() -> Self {
        Self {
            message_counter: HashMap::new(),
            current_timestamp: Timestamp::Bottom,
        }
    }

    fn increment_message_count(&mut self, timestamp: &Timestamp) {
        let count = self.message_counter.entry(timestamp.clone()).or_insert(0);
        *count += 1;
    }

    fn get_message_count(&self, timestamp: &Timestamp) -> usize {
        *self.message_counter.get(timestamp).unwrap_or_else(|| &0)
    }
}

impl StateT for SinkOperatorState {
    fn commit(&mut self, timestamp: &Timestamp) {
        self.current_timestamp = timestamp.clone();
    }

    fn get_last_committed_timestamp(&self) -> Timestamp {
        self.current_timestamp.clone()
    }
}

impl Sink<SinkOperatorState, usize> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<SinkOperatorState>, data: &usize) {
        let timestamp = ctx.get_timestamp().clone();
        tracing::info!("SinkOperator @ {:?}: Received {}", timestamp, data);
        ctx.get_state().increment_message_count(&timestamp);
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<SinkOperatorState>) {
        let timestamp = ctx.get_timestamp().clone();
        tracing::info!(
            "SinkOperator @ {:?}: Received {} data messages.",
            timestamp,
            ctx.get_state().get_message_count(&timestamp),
        );
    }
}

fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let source_config = OperatorConfig::new().name("SourceOperator");
    let source_stream = erdos::connect_source(SourceOperator::new, || {}, source_config);

    let (split_stream_less_50, split_stream_greater_50) = source_stream
        .map(|x: &usize| -> usize { 2 * x })
        .filter(|x: &usize| -> bool { x > &10 })
        .split(|x: &usize| -> bool { x < &50 });

    let left_sink_config = OperatorConfig::new().name("LeftSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        SinkOperatorState::new,
        left_sink_config,
        &split_stream_less_50,
    );

    let right_sink_config = OperatorConfig::new().name("RightSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        SinkOperatorState::new,
        right_sink_config,
        &split_stream_greater_50,
    );

    node.run();
}
