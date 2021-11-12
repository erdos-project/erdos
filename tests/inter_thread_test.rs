use std::thread;

use slog;

use erdos::{
    self,
    dataflow::{
        message::*,
        stream::{
            errors::{ReadError, SendError, TryReadError},
            ExtractStream, IngestStream, WriteStreamT,
        },
        Operator, OperatorConfig, ReadStream, Timestamp, WriteStream,
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
}

impl Operator for SendOperator {
    fn run(&mut self) {
        for count in 0..5 {
            println!("SendOperator: sending {}", count);
            self.write_stream
                .send(Message::new_message(
                    Timestamp::Time(vec![count as u64]),
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

    pub fn msg_callback(_t: &Timestamp, data: &usize) {
        println!("RecvOperator: received {}", data);
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

    pub fn msg_callback(t: &Timestamp, data: &usize, write_stream: &mut WriteStream<usize>) {
        println!("SquareOperator: received {}", data);
        write_stream
            .send(Message::new_message(t.clone(), data * data))
            .unwrap();
    }
}

impl Operator for SquareOperator {}

pub struct DestroyOperator {}

impl DestroyOperator {
    pub fn new(_config: OperatorConfig<()>, read_stream: ReadStream<usize>) -> Self {
        read_stream.add_callback(Self::msg_callback);
        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<usize>) {}

    pub fn msg_callback(_t: &Timestamp, data: &usize) {
        let logger = erdos::get_terminal_logger();
        tracing::debug!("DestroyOperator: received {}", data);
    }
}

impl Operator for DestroyOperator {
    fn destroy(&mut self) {
        let logger = erdos::get_terminal_logger();
        tracing::debug!("DestroyOperator: called destroy()");
    }
}

/// Noop operator which may flow watermarks.
pub struct TwoStreamDestroyOperator {}

impl TwoStreamDestroyOperator {
    pub fn new(
        _config: OperatorConfig<()>,
        _left_stream: ReadStream<usize>,
        _right_stream: ReadStream<usize>,
        _write_stream: WriteStream<bool>,
    ) -> Self {
        Self {}
    }

    pub fn connect(
        _left_stream: &ReadStream<usize>,
        _right_stream: &ReadStream<usize>,
    ) -> WriteStream<bool> {
        WriteStream::new()
    }
}

impl Operator for TwoStreamDestroyOperator {
    fn destroy(&mut self) {
        let logger = erdos::get_terminal_logger();
        tracing::debug!("TwoStreamDestroyOperator: called destroy()");
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
                Timestamp::Time(vec![count as u64]),
                count,
            ))
            .unwrap();
    }
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
                Timestamp::Time(vec![count as u64]),
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
        let msg = Message::new_message(Timestamp::Time(vec![count as u64]), count);
        tracing::debug!("Ingest stream: sending {:?}", msg);
        ingest_stream.send(msg).unwrap();
        let result = extract_stream.read();
        tracing::debug!("Received {:?}", result);
    }
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

    let msg = Message::new_message(Timestamp::Time(vec![0]), 0);
    tracing::debug!("IngestStream: sending {:?}", msg);
    ingest_stream.send(msg).unwrap();

    let received_msg = extract_stream.read().unwrap();
    tracing::debug!("ExtractStream: received {:?}", received_msg);

    let msg = Message::new_watermark(Timestamp::Top);
    tracing::debug!("IngestStream: sending {:?}", msg);
    ingest_stream.send(msg).unwrap();

    let received_msg = extract_stream.read().unwrap();
    tracing::debug!("ExtractStream: received {:?}", received_msg);

    // Check that the stream is closed.
    let msg = Message::new_message(Timestamp::Time(vec![0]), 0);
    assert!(ingest_stream.is_closed());
    assert!(extract_stream.is_closed());
    assert_eq!(ingest_stream.send(msg), Err(SendError::Closed));
    assert_eq!(extract_stream.read(), Err(ReadError::Closed));
    assert_eq!(extract_stream.try_read(), Err(TryReadError::Closed));
}

#[test]
fn test_only_destroy_if_closed() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    // Ingest streams go out of scope, but does not close.
    let mut extract_stream = {
        let mut ingest_stream_1 = IngestStream::new(0);
        let mut ingest_stream_2 = IngestStream::new(0);
        let s = connect_1_write!(
            TwoStreamDestroyOperator,
            OperatorConfig::new(),
            ingest_stream_1,
            ingest_stream_2
        );
        let extract_stream = ExtractStream::new(0, &s);

        node.run_async();

        let watermark_msg = Message::new_watermark(Timestamp::Time(vec![1]));
        ingest_stream_1.send(watermark_msg.clone()).unwrap();
        ingest_stream_2.send(watermark_msg.clone()).unwrap();

        // Only close 1 of the ingest streams.
        ingest_stream_1
            .send(Message::new_watermark(Timestamp::Top))
            .unwrap();

        extract_stream
    };

    // Receive 1 flowed watermark message for t=1.
    let msg = extract_stream.read().unwrap();
    assert_eq!(msg, Message::new_watermark(Timestamp::Time(vec![1])));
    assert_eq!(extract_stream.try_read(), Err(TryReadError::Empty));
    assert!(!extract_stream.is_closed());
}

/// Test that an operator's write streams are also destroyed.
#[test]
fn test_close_write_streams_on_destroy() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let mut ingest_stream_1 = IngestStream::new(0);
    let mut ingest_stream_2 = IngestStream::new(0);
    let s = connect_1_write!(
        TwoStreamDestroyOperator,
        OperatorConfig::new(),
        ingest_stream_1,
        ingest_stream_2
    );
    let mut extract_stream = ExtractStream::new(0, &s);

    node.run_async();

    let watermark_msg = Message::new_watermark(Timestamp::Time(vec![1]));
    ingest_stream_1.send(watermark_msg.clone()).unwrap();
    ingest_stream_2.send(watermark_msg).unwrap();

    // Receive flowed watermark message for t=1.
    let msg = extract_stream.read().unwrap();
    assert_eq!(Message::new_watermark(Timestamp::Time(vec![1])), msg);

    // Close both ingest streams.
    ingest_stream_1
        .send(Message::new_watermark(Timestamp::Top))
        .unwrap();
    ingest_stream_2
        .send(Message::new_watermark(Timestamp::Top))
        .unwrap();
    assert!(ingest_stream_1.is_closed());
    assert!(ingest_stream_2.is_closed());

    // Receive top watermark.
    let msg = extract_stream.read().unwrap();
    assert_eq!(msg, Message::new_watermark(Timestamp::Top));
    assert_eq!(extract_stream.try_read(), Err(TryReadError::Closed));
    assert!(extract_stream.is_closed());
}
