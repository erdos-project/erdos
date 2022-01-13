/// Publishes erdos messages of type String to ROS topic "chatter".
/// Pipeline is as follows:
/// ERDOS SourceOperator -> ToRosOperator converts and publishes ROS messages
extern crate erdos;

use std::{thread, time::Duration};

use erdos::dataflow::operator::*;
use erdos::dataflow::operators::ros::*;
use erdos::dataflow::stream::WriteStreamT;
use erdos::dataflow::Message;
use erdos::dataflow::*;
use erdos::node::Node;
use erdos::Configuration;

struct SourceOperator {}

impl SourceOperator {
    pub fn new() -> Self {
        Self {}
    }
}

// This Source operator repeatedly sends String messages.
impl Source<(), String> for SourceOperator {
    fn run(&mut self, write_stream: &mut WriteStream<String>) {
        tracing::info!("Running Source Operator");
        for t in 0..10 {
            let timestamp = Timestamp::Time(vec![t as u64]);
            write_stream
                .send(Message::new_message(
                    timestamp.clone(),
                    String::from("Hello from ERDOS"),
                ))
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

// Defines a function that converts an ERDOS message containing String data to a ROS String message.
fn erdos_to_ros(input: &Message<String>) -> rosrust_msg::std_msgs::String {
    rosrust_msg::std_msgs::String {
        data: input.data.to_string(),
    }
}

fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    // Creates a Source node on the ERDOS side which contains the messages of interest to publish to ROS.
    let source_config = OperatorConfig::new().name("SourceOperator");
    let source_stream = erdos::connect_source(SourceOperator::new, || {}, source_config);

    // Connects a ToRosOperator as a Sink node in the ERDOS pipeline.
    // The operator will convert the messages using conversion function above, and publish the messages on the ROS topic "chatter".
    let ros_sink_config = OperatorConfig::new().name("ToRosOperator");
    erdos::connect_sink(
        move || -> ToRosOperator<String, rosrust_msg::std_msgs::String> {
            ToRosOperator::new("chatter", erdos_to_ros)
        },
        || {},
        ros_sink_config,
        &source_stream,
    );

    node.run();
}
