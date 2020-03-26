#![feature(custom_test_frameworks)]
#![test_runner(criterion::runner)]
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime},
};

use criterion::{AxisScale, Criterion, PlotConfiguration};
use criterion_macro::criterion;
use serde::{Deserialize, Serialize};

use erdos::{
    self,
    dataflow::{
        message::*,
        operators::{JoinOperator, MapOperator},
        stream::{ExtractStream, IngestStream, WriteStreamT},
        Operator, OperatorConfig, ReadStream, WriteStream,
    },
};

mod utils;
use utils::{BenchType, DataflowHandle};

static P_VALUES: [f32; 5] = [0.5, 0.75, 0.9, 0.95, 0.99];

/// A multi-threaded operator which measures the latency of messages
/// received and forwards these latencies upon receiving a watermark
/// on all input streams.
///
/// Assumes that all input streams send a watermark before the next
/// message is sent.
struct FiveStreamReceiver {}

impl FiveStreamReceiver {
    pub fn new(
        _config: OperatorConfig<()>,
        read_stream_1: ReadStream<Vec<SenderData>>,
        read_stream_2: ReadStream<Vec<SenderData>>,
        read_stream_3: ReadStream<Vec<SenderData>>,
        read_stream_4: ReadStream<Vec<SenderData>>,
        read_stream_5: ReadStream<Vec<SenderData>>,
        write_stream: WriteStream<Vec<Duration>>,
    ) -> Self {
        let read_streams = [
            read_stream_1,
            read_stream_2,
            read_stream_3,
            read_stream_4,
            read_stream_5,
        ];
        let mut stateful_read_streams = VecDeque::new();
        for read_stream in read_streams.iter() {
            let stateful_read_stream = read_stream.add_state(Vec::new());
            stateful_read_stream.add_callback(Self::data_callback);
            stateful_read_streams.push_back(stateful_read_stream);
        }

        erdos::add_watermark_callback!(
            (
                stateful_read_streams.pop_front().unwrap(),
                stateful_read_streams.pop_front().unwrap(),
                stateful_read_streams.pop_front().unwrap(),
                stateful_read_streams.pop_front().unwrap(),
                stateful_read_streams.pop_front().unwrap()
            ),
            (write_stream),
            (Self::watermark_callback)
        );

        Self {}
    }

    pub fn connect(
        _read_stream_1: &ReadStream<Vec<SenderData>>,
        _read_stream_2: &ReadStream<Vec<SenderData>>,
        _read_stream_3: &ReadStream<Vec<SenderData>>,
        _read_stream_4: &ReadStream<Vec<SenderData>>,
        _read_stream_5: &ReadStream<Vec<SenderData>>,
    ) -> WriteStream<Vec<Duration>> {
        WriteStream::new()
    }

    pub fn data_callback(t: Timestamp, sender_datas: Vec<SenderData>, state: &mut Vec<Duration>) {
        let recv_time = SystemTime::now();
        *state = sender_datas
            .into_iter()
            .map(|data| recv_time.duration_since(data.send_time).unwrap())
            .collect();
    }

    pub fn watermark_callback(
        t: &Timestamp,
        state_1: &Vec<Duration>,
        state_2: &Vec<Duration>,
        state_3: &Vec<Duration>,
        state_4: &Vec<Duration>,
        state_5: &Vec<Duration>,
        write_stream: &mut WriteStream<Vec<Duration>>,
    ) {
        let result: Vec<Duration> = state_1
            .iter()
            .copied()
            .chain(state_2.iter().copied())
            .chain(state_3.iter().copied())
            .chain(state_4.iter().copied())
            .chain(state_5.iter().copied())
            .collect();
        let msg = Message::new_message(t.clone(), result);
        write_stream.send(msg).unwrap();
    }
}

impl Operator for FiveStreamReceiver {}

/// Used to measure the latency of a message.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SenderData {
    send_time: SystemTime,
    data: Vec<u8>,
}

impl SenderData {
    fn new(msg_size: usize) -> Self {
        let data = vec![0; msg_size];
        Self {
            send_time: SystemTime::now(),
            data,
        }
    }
}

/// Added to a [`MapOperator`] to create senders.
fn sender_fn(msg_size: usize) -> Vec<SenderData> {
    vec![SenderData::new(msg_size)]
}

/// Added to a [`JoinOperator`] to join data sent by senders.
fn join_fn(left: Vec<Vec<SenderData>>, right: Vec<Vec<SenderData>>) -> Vec<SenderData> {
    left.into_iter()
        .flatten()
        .chain(right.into_iter().flatten())
        .collect()
}

/// Added to a [`MapOperator`] to create receivers.
/// Measures the latencies of the potentially joined messages and sends the result.
fn receiver_fn(sender_datas: Vec<SenderData>) -> Vec<Duration> {
    let recv_time = SystemTime::now();
    sender_datas
        .into_iter()
        .map(|data| recv_time.duration_since(data.send_time).unwrap())
        .collect()
}

/// Sets up a dataflow where each sender sends messages to each receiver,
/// and measures the latency for each message.
fn setup_latency_dataflow(
    num_senders: usize,
    num_receivers: usize,
    bench_type: BenchType,
) -> DataflowHandle {
    let (mut node_count, node_increment) = match bench_type {
        BenchType::InterThread => (0, 0),
        BenchType::InterProcess => (1, 1),
    };

    let ingest_stream = IngestStream::new(0);
    let mut streams_to_join = VecDeque::new();
    for i in 0..num_senders {
        let send_stream = erdos::connect_1_write!(
            MapOperator<usize, Vec<SenderData>>,
            OperatorConfig::new()
                .name(&format!("Sender {}", i))
                .node(node_count)
                .arg(sender_fn),
            ingest_stream
        );
        streams_to_join.push_back(send_stream);
        node_count += node_increment;
    }

    let mut extract_streams = Vec::new();
    for i in 0..num_receivers {
        // Co-locates the JoinOperators and the ReceiveOperator.
        // Do a tree-join for parallelism.
        let mut streams_to_join_clone = streams_to_join.clone();
        let mut join_count = 0;
        while streams_to_join_clone.len() > 1 {
            let left_stream = streams_to_join_clone.pop_front().unwrap();
            let right_stream = streams_to_join_clone.pop_front().unwrap();
            let joined_stream = erdos::connect_1_write!(
                JoinOperator<Vec<SenderData>, Vec<SenderData>, Vec<SenderData>>,
                OperatorConfig::new()
                    .name(&format!("Join {}.{}", i, join_count))
                    .node(node_count)
                    .arg(join_fn),
                left_stream,
                right_stream
            );
            streams_to_join_clone.push_back(joined_stream);
            join_count += 1;
        }
        assert_eq!(streams_to_join_clone.len(), 1);
        let joined_stream = streams_to_join_clone.pop_front().unwrap();
        let duration_stream = erdos::connect_1_write!(
            MapOperator<Vec<SenderData>, Vec<Duration>>,
            OperatorConfig::new()
                .name(&format!("Receiver {}", i))
                .node(node_count)
                .arg(receiver_fn),
            joined_stream
        );
        extract_streams.push(ExtractStream::new(0, &duration_stream));

        node_count += node_increment;
    }

    DataflowHandle::new(node_count, ingest_stream, extract_streams)
}

/// Sets up a dataflow from 5 senders to 1 [`FiveStreamReceiver`].
fn setup_multi_threaded_op_latency_dataflow(bench_type: BenchType) -> DataflowHandle {
    let (mut node_count, node_increment) = match bench_type {
        BenchType::InterThread => (0, 0),
        BenchType::InterProcess => (1, 1),
    };

    let ingest_stream = IngestStream::new(0);
    let mut streams = VecDeque::new();
    for i in 0..5 {
        let send_stream = erdos::connect_1_write!(
            MapOperator<usize, Vec<SenderData>>,
            OperatorConfig::new()
                .name(&format!("Sender {}", i))
                .node(node_count)
                .arg(sender_fn),
            ingest_stream
        );
        streams.push_back(send_stream);
        node_count += node_increment;
    }

    // Need to unpack because macro takes identifiers for read streams.
    let send_stream_1 = streams.pop_front().unwrap();
    let send_stream_2 = streams.pop_front().unwrap();
    let send_stream_3 = streams.pop_front().unwrap();
    let send_stream_4 = streams.pop_front().unwrap();
    let send_stream_5 = streams.pop_front().unwrap();

    let result_stream = erdos::connect_1_write!(
        FiveStreamReceiver,
        OperatorConfig::new()
            .name("FiveStreamReceiver")
            .node(node_count),
        send_stream_1,
        send_stream_2,
        send_stream_3,
        send_stream_4,
        send_stream_5
    );
    node_count += 1;

    let extract_stream = ExtractStream::new(0, &result_stream);

    DataflowHandle::new(node_count, ingest_stream, vec![extract_stream])
}

/// Runs 1 iteration of the dataflow and returns the measured latency at the p-value.
fn measure_p_latency(handle: &mut DataflowHandle, time: u64, p: f32) -> Duration {
    let msg = Message::new_message(Timestamp::new(vec![time]), 100_000);
    handle.ingest_stream.send(msg).unwrap();
    let watermark = Message::new_watermark(Timestamp::new(vec![time]));
    handle.ingest_stream.send(watermark).unwrap();

    let mut durations: Vec<Duration> = Vec::new();
    for extract_stream in handle.extract_streams.iter_mut() {
        // Get latencies from corresponding receiver.
        let msg = extract_stream.read().unwrap();
        durations.extend(msg.data().unwrap().iter());
        // Get watermark.
        extract_stream.read().unwrap();
    }

    durations.sort();

    let idx = if p >= 1.0 {
        durations.len() - 1
    } else {
        (durations.len() as f32 * p).floor() as usize
    };
    durations[idx]
}

/// Varies the size of a message and measures the resulting latency on a
/// dataflow with 1 sender and 1 receiver.
fn benchmark_msg_size(c: &mut Criterion, bench_type: BenchType) {
    erdos::reset();
    let mut handle = setup_latency_dataflow(1, 1, bench_type);
    handle.run();
    let mut time = 0;

    let mut group = match bench_type {
        BenchType::InterProcess => c.benchmark_group("message size vs latency (inter-process)"),
        BenchType::InterThread => c.benchmark_group("message size vs latency (inter-thread)"),
    };
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for exp in 0..=5 {
        let msg_size = 10usize.pow(exp);

        group.bench_function(format!("10^{} B", exp), |b| {
            b.iter_custom(|iters| {
                let mut duration = Duration::from_nanos(0);
                for _ in 0..iters {
                    let msg = Message::new_message(Timestamp::new(vec![time]), msg_size);
                    handle.ingest_stream.send(msg).unwrap();
                    let result = handle.extract_streams[0].read().unwrap();
                    duration += result.data().unwrap()[0];
                    time += 1;
                }
                duration
            })
        });
    }

    handle.shutdown();
}

/// Varies the number of senders sending messages to 1 receiver
/// and reports the latency at several p-values.
fn benchmark_num_senders(c: &mut Criterion, bench_type: BenchType) {
    let mut group = match bench_type {
        BenchType::InterProcess => c.benchmark_group("num senders vs. latency (inter-process)"),
        BenchType::InterThread => c.benchmark_group("num senders vs. latency (inter-thread)"),
    };

    for num_senders in 1..=5 {
        erdos::reset();
        let mut handle = setup_latency_dataflow(num_senders, 1, bench_type);
        handle.run();
        let mut time = 0;

        for p in &P_VALUES {
            group.bench_function(format!("{} senders (p={})", num_senders, p), |b| {
                b.iter_custom(|iters| {
                    let mut duration = Duration::from_nanos(0);
                    for _ in 0..iters {
                        duration += measure_p_latency(&mut handle, time, *p);
                        time += 1;
                    }
                    duration
                });
            });
        }

        handle.shutdown();
    }
}

/// Varies the number of receivers receiving messages from 1 sender
/// and reports the latency at several p-values.
fn benchmark_num_receivers(c: &mut Criterion, bench_type: BenchType) {
    let mut group = match bench_type {
        BenchType::InterProcess => c.benchmark_group("num receivers vs. latency (inter-process)"),
        BenchType::InterThread => c.benchmark_group("num receivers vs. latency (inter-thread)"),
    };

    for num_receivers in 1..=5 {
        erdos::reset();
        let mut handle = setup_latency_dataflow(1, num_receivers, bench_type);
        handle.run();
        let mut time = 0;

        for p in &P_VALUES {
            group.bench_function(format!("{} receivers (p={})", num_receivers, p), |b| {
                b.iter_custom(|iters| {
                    let mut duration = Duration::from_nanos(0);
                    for _ in 0..iters {
                        duration += measure_p_latency(&mut handle, time, *p);
                        time += 1;
                    }
                    duration
                });
            });
        }

        handle.shutdown();
    }
}

/// Measure the latency of messages sent from 5 senders to 1 multi-threaded receiver
/// at several p-values.
fn benchmark_multi_threaded_operator(c: &mut Criterion, bench_type: BenchType) {
    let mut group = match bench_type {
        BenchType::InterProcess => {
            c.benchmark_group("multi-threaded operator latency (inter-process)")
        }
        BenchType::InterThread => {
            c.benchmark_group("multi-threaded operator latency (inter-thread)")
        }
    };

    erdos::reset();
    let mut handle = setup_multi_threaded_op_latency_dataflow(bench_type);
    handle.run();
    let mut time = 0;

    for p in &P_VALUES {
        group.bench_function(format!("p={}", p), |b| {
            b.iter_custom(|iters| {
                let mut duration = Duration::from_nanos(0);
                for _ in 0..iters {
                    duration += measure_p_latency(&mut handle, time, *p);
                    time += 1;
                }
                duration
            })
        });
    }

    handle.shutdown();
}

#[criterion]
fn benchmark_latency(c: &mut Criterion) {
    for bench_type in vec![BenchType::InterProcess, BenchType::InterThread].into_iter() {
        benchmark_msg_size(c, bench_type);
        benchmark_num_senders(c, bench_type);
        benchmark_num_receivers(c, bench_type);
        benchmark_multi_threaded_operator(c, bench_type);
    }
}
