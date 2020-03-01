extern crate erdos;
use erdos::dataflow::{
    operators::JoinOperator,
    operators::MapOperator,
    stream::{ExtractStream, WriteStreamT},
    Message, OperatorConfig, ReadStream, Timestamp, WriteStream,
};
use erdos::node::Node;
use erdos::*;
use std::thread;

mod utils;

pub struct InputGenOp {
    name: String,
    output_stream: WriteStream<u32>,
}

impl InputGenOp {
    pub fn new(config: OperatorConfig<()>, output_stream: WriteStream<u32>) -> Self {
        Self {
            name: config.name,
            output_stream,
        }
    }

    pub fn connect() -> WriteStream<u32> {
        WriteStream::new()
    }

    pub fn run(&mut self) {
        for i in 0..10 {
            self.output_stream
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i));
            self.output_stream
                .send(Message::new_watermark(Timestamp::new(vec![i as u64])));
        }
    }
}
#[test]
fn test_input_receiver_map() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let input_config = OperatorConfig::new("InputOperator", (), true, 0);
    let s1 = connect_1_write!(InputGenOp, input_config);
    let map_config = OperatorConfig::new(
        "MapOperator",
        |data: u32| -> u64 { (data * 2) as u64 },
        true,
        0,
    );
    let s2 = connect_1_write!(MapOperator<u32, u64>, map_config, s1);
    let mut extract_stream = ExtractStream::new(0, &s2);

    node.run_async();

    let mut i = 0;
    while i < 10 {
        let msg = extract_stream.read();
        if let Message::TimestampedData(data) = msg.unwrap() {
            assert_eq!(
                data.data,
                i * 2,
                "The returned value ({}) was different than expected ({}).",
                data.data,
                i * 2
            );
            i += 1;
        }
    }
}

// Join Operator Tests.
#[test]
fn test_input_receiver_join() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let input_config_left = OperatorConfig::new("InputOperator_Left", (), true, 0);
    let s1 = connect_1_write!(InputGenOp, input_config_left);

    let input_config_right = OperatorConfig::new("InputOperator_Right", (), true, 0);
    let s2 = connect_1_write!(InputGenOp, input_config_right);
    let join_config = OperatorConfig::new(
        "JoinOperator",
        |left_data: Vec<u32>, right_data: Vec<u32>| -> u64 {
            (left_data.iter().sum::<u32>() + right_data.iter().sum::<u32>()) as u64
        },
        true,
        0,
    );
    let s3 = connect_1_write!(JoinOperator<u32, u32, u64>, join_config, s1, s2);
    let mut extract_stream = ExtractStream::new(0, &s3);

    node.run_async();

    let mut i = 0;
    while i < 10 {
        let msg = extract_stream.read();
        if let Message::TimestampedData(data) = msg.unwrap() {
            assert_eq!(
                data.data,
                i * 2,
                "The returned value ({}) was different than expected ({}).",
                data.data,
                i * 2
            );
            i += 1;
        }
    }
}
