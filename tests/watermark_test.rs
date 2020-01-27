use std::thread;

use erdos::{
    dataflow::{
        message::*, operators::MapOperator, stream::WriteStreamT, OperatorConfig, ReadStream,
        WriteStream,
    },
    node::Node,
    *,
};

mod utils;

/// Sends 5 watermark messages.
pub struct SendOperator {
    write_stream: WriteStream<usize>,
}

impl SendOperator {
    pub fn new(_config: OperatorConfig<()>, write_stream: WriteStream<usize>) -> Self {
        Self { write_stream }
    }

    pub fn connect() -> WriteStream<usize> {
        WriteStream::new()
    }

    pub fn run(&mut self) {
        for count in 0..5 {
            println!("SendOperator: sending {}", count);
            self.write_stream
                .send(Message::new_watermark(Timestamp::new(vec![count as u64])))
                .unwrap();
        }
    }
}

/// Prints a message after receiving a watermark.
pub struct RecvOperator {}

impl RecvOperator {
    pub fn new(config: OperatorConfig<bool>, read_stream: ReadStream<usize>) -> Self {
        let should_flow_watermarks = config.arg;
        erdos::add_watermark_callback!(
            (read_stream),
            (),
            (RecvOperator::watermark_callback),
            should_flow_watermarks
        );
        // read_stream.add_watermark_callback(RecvOperator::watermark_callback);
        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<usize>) {}

    pub fn run(&self) {}

    pub fn watermark_callback(t: &Timestamp, should_flow_watermarks: &mut bool) {
        if *should_flow_watermarks {
            println!("RecvOperator: received watermark: {:?}", t);
        } else {
            panic!(
                "RecvOperator: should not receive watermark, but received one anyway {:?}",
                t
            )
        }
    }
}

#[test]
fn test_flow_watermarks() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let s1 = connect_1_write!(SendOperator, ());
    let s2 = connect_1_write!(MapOperator<usize, usize>, (), s1);
    connect_0_write!(RecvOperator, true, s2);

    node.run_async();

    thread::sleep(std::time::Duration::from_millis(2000));
}

#[test]
fn test_no_flow_watermarks() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let s1 = connect_1_write!(SendOperator, ());
    let map_config = OperatorConfig::new("MapOperator", (), false, 0);
    let s2 = connect_1_write!(MapOperator<usize, usize>, map_config, s1);
    connect_0_write!(RecvOperator, false, s2);

    node.run_async();

    thread::sleep(std::time::Duration::from_millis(2000));
}
