extern crate erdos;

use std::{thread, time::Duration};

use erdos::dataflow::operator::*;
use erdos::dataflow::operators::*;
use erdos::dataflow::stream::WriteStreamT;
use erdos::dataflow::*;
use erdos::dataflow::Message;
use erdos::node::Node;
use erdos::Configuration;

struct SourceOperator {}

impl SourceOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Source<(), String> for SourceOperator {
    fn run(&mut self, write_stream: &mut WriteStream<String>) {
        let logger = erdos::get_terminal_logger();
        slog::info!(logger, "Running Source Operator");
        for t in 0..10 {
            let timestamp = Timestamp::Time(vec![t as u64]);
            write_stream
                .send(Message::new_message(timestamp.clone(), String::from("hello world")))
                .unwrap();
            write_stream
                .send(Message::new_watermark(timestamp))
                .unwrap();
            thread::sleep(Duration::from_millis(100));
            // thread::sleep(Duration::new(5, 0));
        }
    }

    fn destroy(&mut self) {
        slog::info!(erdos::get_terminal_logger(), "Destroying Source Operator");
    }
}


fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let source_config = OperatorConfig::new().name("SourceOperator");
    let source_stream = erdos::connect_source(SourceOperator::new, || {}, source_config);

    let erdos_to_ros = |input: &String| -> rosrust::RawMessage {
        rosrust::RawMessage(Vec::from(input.to_string().as_bytes()))
    };

    let ros_sink_config = OperatorConfig::new().name("ToRosOperator");
    erdos::connect_sink(
        move || -> ToRosOperator<String, rosrust::RawMessage> { ToRosOperator::new("chatter", erdos_to_ros) },
        || {}, 
        ros_sink_config,
        &source_stream,
    );

    // for i in 0..10 {
    //     source_stream.send(Message::new_message(Timestamp::new(vec![i as u64]), i)).unwrap();
    // }

    node.run();
}
