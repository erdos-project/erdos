#[macro_use]
extern crate erdos;

use erdos::dataflow::{
    operators::{JoinOperator, SourceOperator},
    stream::IngestStream,
    Data, Message, Operator, OperatorConfig, ReadStream, Timestamp, WriteStream,
};
use erdos::deadlines::*;
use erdos::node::Node;
use erdos::Configuration;
use thread::sleep_ms;

use std::thread;
use std::time::Duration;

struct DeadlineOperator {}

impl DeadlineOperator {
    pub fn new(
        config: OperatorConfig<()>,
        read_stream: ReadStream<usize>,
        write_stream: WriteStream<usize>,
    ) -> Self {
        let mut receiving_deadline =
            TimestampReceivingFrequencyDeadline::new(Duration::from_millis(100), || {
                slog::error!(
                    erdos::get_terminal_logger(),
                    "Missed timestamp receiving frequency deadline!"
                );
            });
        receiving_deadline.subscribe(&read_stream).unwrap();
        // config is more like a context
        config.add_deadline(receiving_deadline);

        let mut sending_deadline =
            TimestampSendingFrequencyDeadline::new(Duration::from_millis(100), || {
                slog::error!(
                    erdos::get_terminal_logger(),
                    "Missed timestamp sending frequency deadline!"
                );
            });
        sending_deadline.subscribe(&write_stream).unwrap();
        config.add_deadline(sending_deadline);

        let mut output_deadline = TimestampOutputDeadline::new(Duration::from_millis(50), || {
            slog::error!(
                erdos::get_terminal_logger(),
                "Missed timestamp output frequency deadline!"
            );
        });
        output_deadline.add_read_stream(&read_stream);
        output_deadline.add_write_stream(&write_stream);
        config.add_deadline(output_deadline);

        Self {}
    }

    pub fn connect(_: &ReadStream<usize>) -> WriteStream<usize> {
        WriteStream::new()
    }

    fn on_data(t: Timestamp, d: usize) {}

    fn on_watermark(t: Timestamp) {}
}

impl Operator for DeadlineOperator {}

fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));
    // let s1 = connect_1_write!(
    //     SourceOperator,
    //     OperatorConfig::new().name("SourceOperator1")
    // );
    // let s2 = connect_1_write!(
    //     SourceOperator,
    //     OperatorConfig::new().name("SourceOperator2")
    // );
    // let _s3 = connect_1_write!(JoinOperator<usize, usize, usize>, OperatorConfig::new().name("JoinOperator").arg(
    //     |left: Vec<usize>, right: Vec<usize>| -> usize {
    //         let left_sum: usize = left.iter().sum();
    //         let right_sum: usize = right.iter().sum();
    //         left_sum + right_sum
    //     }), s1, s2);

    let mut ingest_stream: IngestStream<usize> = IngestStream::new(0);

    let _s = connect_1_write!(
        DeadlineOperator,
        OperatorConfig::new().name("DeadlineOperator"),
        ingest_stream
    );

    node.run_async();
    thread::sleep_ms(10);

    println!("Sending 0: deadline should not trigger.");
    ingest_stream
        .send(Message::Watermark(Timestamp::new(vec![0u64])))
        .unwrap();
    thread::sleep_ms(50);

    println!("Sending 1: deadline should trigger.");
    ingest_stream
        .send(Message::Watermark(Timestamp::new(vec![1u64])))
        .unwrap();
    thread::sleep_ms(150);

    // Should make all deadlines.
    println!("Sending data 2: deadline should not trigger.");
    ingest_stream
        .send(Message::new_message(Timestamp::new(vec![2u64]), 0))
        .unwrap();
    println!("Sending watermark 2: deadline should not trigger.");
    ingest_stream
        .send(Message::new_watermark(Timestamp::new(vec![2u64])))
        .unwrap();
    // Should miss output deadline.
    println!("Sending data 3: deadline should trigger.");
    ingest_stream
        .send(Message::new_message(Timestamp::new(vec![3u64]), 0))
        .unwrap();
    thread::sleep_ms(75);
}
