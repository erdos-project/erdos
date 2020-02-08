use erdos::{
    dataflow::{
        message::*,
        operators::MapOperator,
        stream::{ExtractStream, WriteStreamT},
        Operator, OperatorConfig, ReadStream, WriteStream,
    },
    node::Node,
    *,
};
use std::thread;

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

impl Operator for SendOperator {}

pub struct MultiStreamCallbackOperator {}

impl MultiStreamCallbackOperator {
    pub fn new(
        _config: OperatorConfig<()>,
        rs1: ReadStream<usize>,
        rs2: ReadStream<usize>,
        ws: WriteStream<usize>,
    ) -> Self {
        erdos::add_watermark_callback!(
            (rs1.add_state(()), rs2.add_state(())),
            (ws),
            (Self::watermark_callback)
        );
        Self {}
    }

    pub fn connect(_rs1: &ReadStream<usize>, _rs2: &ReadStream<usize>) -> WriteStream<usize> {
        WriteStream::new()
    }

    pub fn watermark_callback(t: &Timestamp, _s1: &(), _s2: &(), ws: &mut WriteStream<usize>) {
        ws.send(Message::new_watermark(t.clone())).unwrap();
    }

    pub fn run(&self) {}
}

impl Operator for MultiStreamCallbackOperator {}

/// Prints a message after receiving a watermark.
pub struct RecvOperator {}

impl RecvOperator {
    pub fn new(config: OperatorConfig<bool>, read_stream: ReadStream<usize>) -> Self {
        let should_flow_watermarks = config.arg.unwrap();
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

impl Operator for RecvOperator {}

#[test]
fn test_flow_watermarks() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let s1 = connect_1_write!(SendOperator, OperatorConfig::new().name("SendOperator"));
    let s2 =
        connect_1_write!(MapOperator<usize, usize>, OperatorConfig::new().name("MapOperator"), s1);
    connect_0_write!(
        RecvOperator,
        OperatorConfig::new().name("RecvOperator").arg(true),
        s2
    );

    node.run_async();

    thread::sleep(std::time::Duration::from_millis(2000));
}

#[test]
fn test_no_flow_watermarks() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let s1 = connect_1_write!(SendOperator, OperatorConfig::new().name("SendOperator"));
    let s2 = connect_1_write!(MapOperator<usize, usize>, OperatorConfig::new().name("MapOperator").flow_watermarks(false), s1);
    connect_0_write!(
        RecvOperator,
        OperatorConfig::new()
            .name("RecvOperator")
            .arg(false)
            .flow_watermarks(false),
        s2
    );

    node.run_async();

    thread::sleep(std::time::Duration::from_millis(2000));
}

#[test]
fn test_multi_stream_watermark_callbacks() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let s1 = connect_1_write!(SendOperator, OperatorConfig::new().name("SendOp1"));
    let s2 = connect_1_write!(SendOperator, OperatorConfig::new().name("SendOp2"));
    let s3 = connect_1_write!(
        MultiStreamCallbackOperator,
        OperatorConfig::new()
            .name("MultiStreamCallbackOperator")
            .flow_watermarks(false),
        s1,
        s2
    );
    let mut extract_stream = ExtractStream::new(0, &s3);

    node.run_async();

    for count in 0..5 {
        let msg = extract_stream.read();
        eprintln!("{:?}", msg);
        assert_eq!(
            msg,
            Some(Message::new_watermark(Timestamp::new(vec![count as u64])))
        );
    }
}
