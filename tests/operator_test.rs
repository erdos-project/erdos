extern crate erdos;
use erdos::dataflow::{
    operators::MapOperator, stream::WriteStreamT, Message, OperatorConfig, ReadStream, State,
    Timestamp, WriteStream,
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
        }
    }
}

#[derive(Clone)]
pub struct ReceiverState {
    counter: u64,
}

pub struct ReceiverOp {
    name: String,
    input_stream: ReadStream<u64>,
}

impl ReceiverOp {
    pub fn new(config: OperatorConfig<()>, input_stream: ReadStream<u64>) -> Self {
        let receiver_state = ReceiverState { counter: 0 };
        let stateful_input_stream = input_stream.add_state(receiver_state);
        stateful_input_stream.add_callback(ReceiverOp::on_data_callback);
        Self {
            name: config.name,
            input_stream,
        }
    }

    pub fn connect(input_stream: &ReadStream<u64>) {}

    pub fn on_data_callback(t: Timestamp, msg: u64, state: &mut ReceiverState) {
        let expected: u64 = state.counter * 2;
        assert_eq!(
            expected, msg,
            "The returned value ({}) was different than the expected ({}).",
            msg, expected
        );
        state.counter += 1;
    }

    pub fn run(&self) {}
}

#[test]
fn test_input_receiver() {
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
    let receive_config = OperatorConfig::new("ReceiverOp", (), true, 0);
    connect_0_write!(ReceiverOp, receive_config, s2);

    node.run_async();

    thread::sleep(std::time::Duration::from_millis(2000));
}
