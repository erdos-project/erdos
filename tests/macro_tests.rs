extern crate erdos;
use erdos::dataflow::{
    stream::{ExtractStream, IngestStream, WriteStreamT},
    Message, Operator, OperatorConfig, ReadStream, Timestamp, WriteStream,
};
use erdos::node::Node;

use erdos::*;

mod utils;

/// Operator that outputs 10 integers on two output streams.
pub struct TwoOutputZeroInputGenerator {
    write_stream_a: WriteStream<u32>,
    write_stream_b: WriteStream<u32>,
}

impl TwoOutputZeroInputGenerator {
    pub fn new(
        _config: OperatorConfig<()>,
        write_stream_a: WriteStream<u32>,
        write_stream_b: WriteStream<u32>,
    ) -> Self {
        Self {
            write_stream_a,
            write_stream_b,
        }
    }

    pub fn connect() -> (WriteStream<u32>, WriteStream<u32>) {
        (WriteStream::new(), WriteStream::new())
    }
}

impl Operator for TwoOutputZeroInputGenerator {
    fn run(&mut self) {
        for i in 0..10 {
            self.write_stream_a
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
            self.write_stream_b
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
        }
    }
}

/// This test ensures that connect_2_write works as expected.
#[test]
fn test_two_output_zero_input_generator() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let (write_stream_a, write_stream_b) = connect_2_write!(
        TwoOutputZeroInputGenerator,
        OperatorConfig::new().name("TwoOutputGenerator")
    );
    let mut extract_stream_a = ExtractStream::new(0, &write_stream_a);
    let mut extract_stream_b = ExtractStream::new(0, &write_stream_b);

    node.run_async();

    for i in 0..10 {
        let msg_a = extract_stream_a.read();
        let msg_b = extract_stream_b.read();

        if let Message::TimestampedData(data) = msg_a.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
        if let Message::TimestampedData(data) = msg_b.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
    }
}

/// Operator that outputs 10 integers on two output streams, with one input stream.
pub struct TwoOutputOneInputGenerator {
    _read_stream: ReadStream<u32>,
    write_stream_a: WriteStream<u32>,
    write_stream_b: WriteStream<u32>,
}

impl TwoOutputOneInputGenerator {
    pub fn new(
        _config: OperatorConfig<()>,
        _read_stream: ReadStream<u32>,
        write_stream_a: WriteStream<u32>,
        write_stream_b: WriteStream<u32>,
    ) -> Self {
        Self {
            _read_stream,
            write_stream_a,
            write_stream_b,
        }
    }

    pub fn connect(_read_stream: &ReadStream<u32>) -> (WriteStream<u32>, WriteStream<u32>) {
        (WriteStream::new(), WriteStream::new())
    }
}

impl Operator for TwoOutputOneInputGenerator {
    fn run(&mut self) {
        for i in 0..10 {
            self.write_stream_a
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
            self.write_stream_b
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
        }
    }
}

/// This test ensures that connect_2_write works as expected.
#[test]
fn test_two_output_one_input_generator() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let ingest_stream = IngestStream::new(0);
    let (write_stream_a, write_stream_b) = connect_2_write!(
        TwoOutputOneInputGenerator,
        OperatorConfig::new().name("TwoOutputGenerator"),
        ingest_stream
    );
    let mut extract_stream_a = ExtractStream::new(0, &write_stream_a);
    let mut extract_stream_b = ExtractStream::new(0, &write_stream_b);

    node.run_async();

    for i in 0..10 {
        let msg_a = extract_stream_a.read();
        let msg_b = extract_stream_b.read();

        if let Message::TimestampedData(data) = msg_a.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
        if let Message::TimestampedData(data) = msg_b.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
    }
}

/// Operator that outputs 10 integers on three output streams.
pub struct ThreeOutputZeroInputGenerator {
    write_stream_a: WriteStream<u32>,
    write_stream_b: WriteStream<u32>,
    write_stream_c: WriteStream<u32>,
}

impl ThreeOutputZeroInputGenerator {
    pub fn new(
        _config: OperatorConfig<()>,
        write_stream_a: WriteStream<u32>,
        write_stream_b: WriteStream<u32>,
        write_stream_c: WriteStream<u32>,
    ) -> Self {
        Self {
            write_stream_a,
            write_stream_b,
            write_stream_c,
        }
    }

    pub fn connect() -> (WriteStream<u32>, WriteStream<u32>, WriteStream<u32>) {
        (WriteStream::new(), WriteStream::new(), WriteStream::new())
    }
}

impl Operator for ThreeOutputZeroInputGenerator {
    fn run(&mut self) {
        for i in 0..10 {
            self.write_stream_a
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
            self.write_stream_b
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
            self.write_stream_c
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
        }
    }
}

/// This test ensures that connect_2_write works as expected.
#[test]
fn test_three_output_zero_input_generator() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let (write_stream_a, write_stream_b, write_stream_c) = connect_3_write!(
        ThreeOutputZeroInputGenerator,
        OperatorConfig::new().name("TwoOutputGenerator")
    );
    let mut extract_stream_a = ExtractStream::new(0, &write_stream_a);
    let mut extract_stream_b = ExtractStream::new(0, &write_stream_b);
    let mut extract_stream_c = ExtractStream::new(0, &write_stream_c);

    node.run_async();

    for i in 0..10 {
        let msg_a = extract_stream_a.read();
        let msg_b = extract_stream_b.read();
        let msg_c = extract_stream_c.read();

        if let Message::TimestampedData(data) = msg_a.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
        if let Message::TimestampedData(data) = msg_b.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
        if let Message::TimestampedData(data) = msg_c.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
    }
}

/// Operator that outputs 10 integers on three output streams, with one input stream.
pub struct ThreeOutputOneInputGenerator {
    _read_stream: ReadStream<u32>,
    write_stream_a: WriteStream<u32>,
    write_stream_b: WriteStream<u32>,
    write_stream_c: WriteStream<u32>,
}

impl ThreeOutputOneInputGenerator {
    pub fn new(
        _config: OperatorConfig<()>,
        read_stream: ReadStream<u32>,
        write_stream_a: WriteStream<u32>,
        write_stream_b: WriteStream<u32>,
        write_stream_c: WriteStream<u32>,
    ) -> Self {
        Self {
            _read_stream: read_stream,
            write_stream_a,
            write_stream_b,
            write_stream_c,
        }
    }

    pub fn connect(
        _read_stream: &ReadStream<u32>,
    ) -> (WriteStream<u32>, WriteStream<u32>, WriteStream<u32>) {
        (WriteStream::new(), WriteStream::new(), WriteStream::new())
    }
}

impl Operator for ThreeOutputOneInputGenerator {
    fn run(&mut self) {
        for i in 0..10 {
            self.write_stream_a
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
            self.write_stream_b
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
            self.write_stream_c
                .send(Message::new_message(Timestamp::new(vec![i as u64]), i))
                .unwrap();
        }
    }
}

/// This test ensures that connect_2_write works as expected.
#[test]
fn test_three_output_one_input_generator() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let ingest_stream = IngestStream::new(0);
    let (write_stream_a, write_stream_b, write_stream_c) = connect_3_write!(
        ThreeOutputOneInputGenerator,
        OperatorConfig::new().name("TwoOutputGenerator"),
        ingest_stream
    );
    let mut extract_stream_a = ExtractStream::new(0, &write_stream_a);
    let mut extract_stream_b = ExtractStream::new(0, &write_stream_b);
    let mut extract_stream_c = ExtractStream::new(0, &write_stream_c);

    node.run_async();

    for i in 0..10 {
        let msg_a = extract_stream_a.read();
        let msg_b = extract_stream_b.read();
        let msg_c = extract_stream_c.read();

        if let Message::TimestampedData(data) = msg_a.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
        if let Message::TimestampedData(data) = msg_b.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
        if let Message::TimestampedData(data) = msg_c.unwrap() {
            assert_eq!(
                data.data, i,
                "The returned value ({}) was different than expected ({}).",
                data.data, i
            );
        }
    }
}
