use std::thread;

use slog;

use erdos::{
    self,
    dataflow::{
        message::*,
        stream::{ExtractStream, IngestStream, WriteStreamT},
        OperatorConfig, ReadStream, WriteStream,
    },
    node::Node,
    *,
};

mod utils;

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
                .send(Message::new_message(
                    Timestamp::new(vec![count as u64]),
                    count,
                ))
                .unwrap();
        }
    }
}

pub struct RecvOperator {}

impl RecvOperator {
    pub fn new(_config: OperatorConfig<()>, read_stream: ReadStream<usize>) -> Self {
        read_stream.add_callback(Self::msg_callback);
        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<usize>) {}

    pub fn run(&self) {}

    pub fn msg_callback(_t: Timestamp, msg: usize) {
        println!("RecvOperator: received {}", msg);
    }
}

pub struct SquareOperator {}

impl SquareOperator {
    pub fn new(
        _config: OperatorConfig<()>,
        read_stream: ReadStream<usize>,
        write_stream: WriteStream<usize>,
    ) -> Self {
        read_stream
            .add_state(write_stream)
            .add_callback(SquareOperator::msg_callback);

        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<usize>) -> WriteStream<usize> {
        WriteStream::new()
    }

    pub fn run(&self) {}

    pub fn msg_callback(t: Timestamp, data: usize, write_stream: &mut WriteStream<usize>) {
        println!("SquareOperator: received {}", data);
        write_stream
            .send(Message::new_message(t, data * data))
            .unwrap();
    }
}

#[test]
fn test_inter_thread() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let config = OperatorConfig::new("SendOperator", (), true, 0);
    let s = connect_1_write!(SendOperator, config.clone());
    connect_0_write!(RecvOperator, config, s);

    node.run_async();
    thread::sleep(std::time::Duration::from_millis(1000));
}

#[test]
fn test_ingest() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let mut ingest_stream = IngestStream::new(0);
    let config = OperatorConfig::new("RecvOperator", (), true, 0);
    connect_0_write!(RecvOperator, config, ingest_stream);

    node.run_async();

    for count in 0..5 {
        println!("IngestStream: sending {}", count);
        ingest_stream
            .send(Message::new_message(
                Timestamp::new(vec![count as u64]),
                count,
            ))
            .unwrap();
    }

    thread::sleep(std::time::Duration::from_millis(1000));
}

#[test]
fn test_extract() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let config = OperatorConfig::new("SendOperator", (), true, 0);
    let s = connect_1_write!(SendOperator, config);
    let mut extract_stream = ExtractStream::new(0, &s);

    node.run_async();

    for count in 0..5 {
        let msg = extract_stream.read();
        assert_eq!(
            msg,
            Some(Message::new_message(
                Timestamp::new(vec![count as u64]),
                count as usize
            ))
        );
    }
}

#[test]
fn test_ingest_extract() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let mut ingest_stream = IngestStream::new(0);
    let config = OperatorConfig::new("SquareOperator", (), true, 0);
    let square_stream = connect_1_write!(SquareOperator, config, ingest_stream);
    let mut extract_stream = ExtractStream::new(0, &square_stream);

    node.run_async();

    let logger = erdos::get_terminal_logger();

    for count in 0..5 {
        let msg = Message::new_message(Timestamp::new(vec![count as u64]), count);
        slog::debug!(logger, "Ingest stream: sending {:?}", msg);
        ingest_stream.send(msg).unwrap();
        let result = extract_stream.read();
        slog::debug!(logger, "Received {:?}", result);
    }

    thread::sleep(std::time::Duration::from_millis(1000));
}
