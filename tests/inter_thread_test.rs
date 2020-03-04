use std::thread;

use slog;

use erdos::{
    self,
    dataflow::{
        message::*,
        stream::{
            errors::{ReadError, TryReadError, WriteStreamError},
            ExtractStream, IngestStream, WriteStreamT,
        },
        Operator, OperatorConfig, ReadStream, WriteStream,
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

impl Operator for SendOperator {}

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

impl Operator for RecvOperator {}

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

impl Operator for SquareOperator {}

pub struct DestroyOperator {}

impl DestroyOperator {
    pub fn new(config: OperatorConfig<()>, read_stream: ReadStream<usize>) -> Self {
        read_stream.add_callback(Self::msg_callback);
        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<usize>) {}

    pub fn msg_callback(t: Timestamp, data: usize) {
        let logger = erdos::get_terminal_logger();
        slog::debug!(logger, "DestroyOperator: received {}", data);
    }
}

impl Operator for DestroyOperator {
    fn destroy(&mut self) {
        let logger = erdos::get_terminal_logger();
        slog::debug!(logger, "DestroyOperator: called destroy()");
    }
}

#[test]
fn test_inter_thread() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let s = connect_1_write!(SendOperator, OperatorConfig::new().name("SendOperator"));
    connect_0_write!(RecvOperator, OperatorConfig::new().name("RecvOperator"), s);

    node.run_async();
    thread::sleep(std::time::Duration::from_millis(1000));
}

#[test]
fn test_shutdown() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let s = connect_1_write!(SendOperator, OperatorConfig::new().name("SendOperator"));
    connect_0_write!(RecvOperator, OperatorConfig::new().name("RecvOperator"), s);

    let node_handle = node.run_async();
    node_handle.shutdown().unwrap();
}

#[test]
fn test_ingest() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let mut ingest_stream = IngestStream::new(0);
    connect_0_write!(
        RecvOperator,
        OperatorConfig::new().name("RecvOperator"),
        ingest_stream
    );

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

    let s = connect_1_write!(SendOperator, OperatorConfig::new().name("SendOperator"));
    let mut extract_stream = ExtractStream::new(0, &s);

    node.run_async();

    for count in 0..5 {
        let msg = extract_stream.read();
        assert_eq!(
            msg,
            Ok(Message::new_message(
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
    let square_stream = connect_1_write!(
        SquareOperator,
        OperatorConfig::new().name("SquareOperator"),
        ingest_stream
    );
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

#[test]
fn test_destroy() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let mut ingest_stream = IngestStream::new(0);
    connect_0_write!(DestroyOperator, OperatorConfig::new(), ingest_stream);
    let mut extract_stream = ExtractStream::new(0, &ReadStream::from(&ingest_stream));

    node.run_async();

    let logger = erdos::get_terminal_logger();

    let msg = Message::new_message(Timestamp::new(vec![0]), 0);
    slog::debug!(logger, "IngestStream: sending {:?}", msg);
    ingest_stream.send(msg).unwrap();

    let received_msg = extract_stream.read().unwrap();
    slog::debug!(logger, "ExtractStream: received {:?}", received_msg);

    let msg = Message::StreamClosed;
    slog::debug!(logger, "IngestStream: sending {:?}", msg);
    ingest_stream.send(msg).unwrap();

    let received_msg = extract_stream.read().unwrap();
    slog::debug!(logger, "ExtractStream: received {:?}", received_msg);

    // Check that the stream is closed.
    let msg = Message::new_message(Timestamp::new(vec![0]), 0);
    assert!(ingest_stream.is_closed());
    assert!(extract_stream.is_closed());
    assert_eq!(ingest_stream.send(msg), Err(WriteStreamError::Closed));
    assert_eq!(extract_stream.read(), Err(ReadError::Closed));
    assert_eq!(extract_stream.try_read(), Err(TryReadError::Closed));

    thread::sleep(std::time::Duration::from_millis(1000));
}
