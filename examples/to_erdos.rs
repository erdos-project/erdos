extern crate erdos;

use erdos::dataflow::context::*;
use erdos::dataflow::operator::*;
use erdos::dataflow::operators::ros::*;
use erdos::dataflow::*;
use erdos::dataflow::Message;
use erdos::node::Node;
use erdos::Configuration;

/// Subscribes to ROS topic named "chatter" which consists of String messages. 
/// Messages are converted and sent on an erdos stream which is captured by a Sink node and printed.
/// Pipeline is as follows:
/// ROS subscriber on topic -> FromRosOperator captures and converts to ERDOS messages -> SinkOperator

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

// This SinkOperator prints out recieved messages in ERDOS pipeline.
impl Sink<(), String> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<()>, data: &String) {
        let timestamp = ctx.get_timestamp().clone();
        tracing::debug!(
            "SinkOperator @ {:?}: Received {}",
            timestamp,
            data,
        );
    }

    fn on_watermark(&mut self, _ctx: &mut SinkContext<()>) {}
}

// Defines a function that converts a ROS String message to an ERDOS message with String data.
fn ros_to_erdos(input: &rosrust_msg::std_msgs::String) -> Message<String> {
    Message::new_message(Timestamp::Time(vec![0 as u64]), String::from(input.data.as_str()))
}

fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    // Creates FromRosOperator which subscribes to topic "chatter" and converts ROS messages to ERDOS messages.
    let ros_source_config = OperatorConfig::new().name("FromRosOperator");
    let ros_source = erdos::connect_source(
        move || -> FromRosOperator<rosrust_msg::std_msgs::String, String> { FromRosOperator::new("chatter", ros_to_erdos) },
        || {},
        ros_source_config,
    );

    // Connects SinkOperator to ERDOS pipeline.
    let erdos_sink_from_ros = OperatorConfig::new().name("SinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        || {},
        erdos_sink_from_ros,
        &ros_source,
    );

    node.run();
}
