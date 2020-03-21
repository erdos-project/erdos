#![feature(custom_test_frameworks)]
#![test_runner(criterion::runner)]
use criterion::{AxisScale, Criterion, PlotConfiguration};
use criterion_macro::criterion;
use std::time::{Duration, SystemTime};

use erdos::{
    self,
    dataflow::{
        message::*,
        stream::{ExtractStream, IngestStream, WriteStreamT},
        Operator, OperatorConfig, ReadStream, WriteStream,
    },
};

mod utils;
use utils::{BenchType, DataflowHandle};

/// Sends a message of the configured size marked with the send time to the receiver
/// whenever it receives a message from its read stream.
struct Sender {}

impl Sender {
    pub fn new(
        _config: OperatorConfig<()>,
        read_stream: ReadStream<usize>,
        write_stream: WriteStream<(SystemTime, Vec<u8>)>,
    ) -> Self {
        read_stream
            .add_state(write_stream)
            .add_callback(Self::msg_callback);
        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<usize>) -> WriteStream<(SystemTime, Vec<u8>)> {
        WriteStream::new()
    }

    pub fn msg_callback(
        t: Timestamp,
        msg_size: usize,
        write_stream: &mut WriteStream<(SystemTime, Vec<u8>)>,
    ) {
        let data_vec = vec![0; msg_size];
        let msg = Message::new_message(t, (SystemTime::now(), data_vec));
        write_stream.send(msg).unwrap();
    }
}

impl Operator for Sender {}

struct Receiver {}

impl Receiver {
    pub fn new(
        _config: OperatorConfig<()>,
        read_stream: ReadStream<(SystemTime, Vec<u8>)>,
        write_stream: WriteStream<Duration>,
    ) -> Self {
        read_stream
            .add_state(write_stream)
            .add_callback(Self::msg_callback);
        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<(SystemTime, Vec<u8>)>) -> WriteStream<Duration> {
        WriteStream::new()
    }

    pub fn msg_callback(
        t: Timestamp,
        data: (SystemTime, Vec<u8>),
        write_stream: &mut WriteStream<Duration>,
    ) {
        let duration = SystemTime::now().duration_since(data.0).unwrap();
        let msg = Message::new_message(t, duration);
        write_stream.send(msg).unwrap();
    }
}

impl Operator for Receiver {}

/// Sets up a dataflow to measure the latency of messages of `msg_size` bytes sent
/// between 2 operators.
/// Returns num nodes.
fn setup_latency_dataflow(bench_type: BenchType) -> DataflowHandle {
    let ingest_stream = IngestStream::new(0);
    let sender_node_id = match bench_type {
        BenchType::InterProcess => 1,
        BenchType::InterThread => 0,
    };
    let measured_stream = erdos::connect_1_write!(
        Sender,
        OperatorConfig::new().node(sender_node_id),
        ingest_stream
    );
    let receiver_node_id = match bench_type {
        BenchType::InterProcess => 2,
        BenchType::InterThread => 0,
    };
    let duration_stream = erdos::connect_1_write!(
        Receiver,
        OperatorConfig::new().node(receiver_node_id),
        measured_stream
    );
    let extract_stream = ExtractStream::new(0, &duration_stream);

    let num_nodes = match bench_type {
        BenchType::InterProcess => 3,
        BenchType::InterThread => 0,
    };
    DataflowHandle::new(num_nodes, ingest_stream, extract_stream)
}

#[criterion]
fn inter_thread_latency(c: &mut Criterion) {
    // Reset dataflow.
    erdos::reset();

    let mut handle = setup_latency_dataflow(BenchType::InterThread);

    handle.run();
    let mut time = 0;

    let mut group = c.benchmark_group("Inter-thread latency");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for exp in 0..=7 {
        let msg_size = 10usize.pow(exp);

        group.bench_function(format!("10^{} B", exp), |b| {
            b.iter_custom(|iters| {
                let mut duration = Duration::from_nanos(0);
                for _ in 0..iters {
                    let msg = Message::new_message(Timestamp::new(vec![time]), msg_size);
                    handle.ingest_stream.send(msg).unwrap();
                    let result = handle.extract_stream.read().unwrap();
                    duration += *result.data().unwrap();
                    time += 1;
                }
                duration
            })
        });
    }

    handle.shutdown();
}

#[criterion]
fn inter_process_latency(c: &mut Criterion) {
    // Reset dataflow.
    erdos::reset();

    let mut handle = setup_latency_dataflow(BenchType::InterProcess);

    handle.run();
    let mut time = 0;

    let mut group = c.benchmark_group("Inter-process latency");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for exp in 0..=7 {
        let msg_size = 10usize.pow(exp);

        group.bench_function(format!("10^{} B", exp), |b| {
            b.iter_custom(|iters| {
                let mut duration = Duration::from_nanos(0);
                for _ in 0..iters {
                    let msg = Message::new_message(Timestamp::new(vec![time]), msg_size);
                    handle.ingest_stream.send(msg).unwrap();
                    let result = handle.extract_stream.read().unwrap();
                    duration += *result.data().unwrap();
                    time += 1;
                }
                duration
            })
        });
    }

    handle.shutdown();
}
