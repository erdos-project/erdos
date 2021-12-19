extern crate erdos;

use std::{collections::HashMap};

use erdos::dataflow::context::*;
use erdos::dataflow::operator::*;
use erdos::dataflow::operators::*;
use erdos::dataflow::*;
use erdos::dataflow::Message;
use erdos::node::Node;
use erdos::Configuration;

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

impl Sink<SinkOperatorState, String> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<SinkOperatorState>, data: &String) {
        let timestamp = ctx.get_timestamp().clone();
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: Received {}",
            timestamp,
            data,
        );
        ctx.get_state().increment_message_count(&timestamp);
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<SinkOperatorState>) {
        let timestamp = ctx.get_timestamp().clone();
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: Received {} data messages.",
            timestamp,
            ctx.get_state().get_message_count(&timestamp),
        );
    }
}


fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let ros_to_erdos = |input: &rosrust_msg::std_msgs::String| -> Message<String> {
        Message::new_message(Timestamp::Time(vec![0 as u64]), String::from(input.data.as_str()))
    }; 

    let ros_source_config = OperatorConfig::new().name("FromRosOperator");
    let ros_source = erdos::connect_source(
        move || -> FromRosOperator<rosrust_msg::std_msgs::String, String> { FromRosOperator::new("chatter", ros_to_erdos) },
        || {},
        ros_source_config,
    );

    let erdos_sink_from_ros = OperatorConfig::new().name("SinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        SinkOperatorState::new,
        erdos_sink_from_ros,
        &ros_source,
    );

    node.run();
}
