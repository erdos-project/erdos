use std::thread;

use erdos::{
    dataflow::{
        message::*, stream::WriteStreamT, LoopStream, Operator, OperatorConfig, ReadStream,
        WriteStream,
    },
    node::Node,
    *,
};

mod utils;

pub struct LoopOperator {
    send_first_msg: bool,
    num_iterations: usize,
    read_stream: ReadStream<usize>,
    write_stream: WriteStream<usize>,
}

impl LoopOperator {
    pub fn new(
        config: OperatorConfig<(bool, usize)>,
        read_stream: ReadStream<usize>,
        write_stream: WriteStream<usize>,
    ) -> Self {
        Self {
            send_first_msg: config.arg.unwrap().0,
            num_iterations: config.arg.unwrap().1,
            read_stream,
            write_stream,
        }
    }

    pub fn connect(_read_stream: &ReadStream<usize>) -> WriteStream<usize> {
        WriteStream::new()
    }

    pub fn run(&mut self) {
        if self.send_first_msg {
            let msg = Message::new_message(Timestamp::new(vec![0]), 0);
            println!("LoopOp: sending {:?}", msg);
            self.write_stream.send(msg).unwrap();
        }

        for _ in 0..self.num_iterations {
            if let Some(Message::TimestampedData(mut timestamped_data)) = self.read_stream.read() {
                println!("LoopOp: received {:?}", timestamped_data);
                timestamped_data.data += 1;
                timestamped_data.timestamp.time[0] += 1;
                println!("LoopOp: sending {:?}", timestamped_data);
                self.write_stream
                    .send(Message::new_message(
                        timestamped_data.timestamp,
                        timestamped_data.data,
                    ))
                    .unwrap();
            }
        }
    }
}

impl Operator for LoopOperator {}

#[test]
fn test_loop() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let loop_stream = LoopStream::new();
    let s1 = connect_1_write!(
        LoopOperator,
        OperatorConfig::new().name("LoopOperator").arg((true, 5)),
        loop_stream
    );
    loop_stream.set(&s1);

    node.run_async();
    thread::sleep(std::time::Duration::from_millis(2000));
}
