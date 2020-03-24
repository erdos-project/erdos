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

fn sender_fn(msg_size: usize) -> Vec<SenderData> {
    println!("sending");
    vec![SenderData::new(msg_size)]
}

fn join_fn(left: Vec<Vec<SenderData>>, right: Vec<Vec<SenderData>>) -> Vec<SenderData> {
    println!("joining");
    left.into_iter()
        .flatten()
        .chain(right.into_iter().flatten())
        .collect()
}

fn receiver_fn(sender_datas: Vec<SenderData>) -> Vec<Duration> {
    let recv_time = SystemTime::now();
    sender_datas
        .into_iter()
        .map(|data| recv_time.duration_since(data.send_time).unwrap())
        .collect()
}

fn setup_latency_dataflow(
    num_senders: usize,
    num_receivers: usize,
    bench_type: &BenchType,
) -> DataflowHandle {
    let (mut node_count, node_increment) = match bench_type {
        BenchType::InterThread => (0, 0),
        BenchType::InterProcess => (1, 1),
    };

    let ingest_stream = IngestStream::new(0);
    let mut streams_to_join = VecDeque::new();
    for i in 0..num_senders {
        // Co-locates the Sender and MapOperator.
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

fn benchmark_msg_size(c: &mut Criterion, bench_type: &BenchType) {
    erdos::reset();
    let mut handle = setup_latency_dataflow(1, 1, bench_type);
    handle.run();
    let mut time = 0;

    let mut group = match bench_type {
        BenchType::InterProcess => c.benchmark_group("Message size vs. Latency (Inter-process)"),
        BenchType::InterThread => c.benchmark_group("Message size vs. Latency (Inter-thread)"),
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

fn measure_p_latency(handle: &mut DataflowHandle, time: u64, p: f32) -> Duration {
    let msg = Message::new_message(Timestamp::new(vec![time]), 10_000);
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

    let idx = if p >= 1.0 {
        durations.len() - 1
    } else {
        (durations.len() as f32 * p).floor() as usize
    };
    durations[idx]
}

fn benchmark_num_senders(c: &mut Criterion, bench_type: &BenchType) {
    let mut group = match bench_type {
        BenchType::InterProcess => c.benchmark_group("Num senders vs. Latency (inter-process)"),
        BenchType::InterThread => c.benchmark_group("Num senders vs. Latency (inter-thread)"),
    };

    for num_senders in 2..=5 {
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

#[criterion]
fn latency_benchmarks(c: &mut Criterion) {
    for bench_type in &[BenchType::InterProcess, BenchType::InterThread] {
        //benchmark_msg_size(c, bench_type);
        benchmark_num_senders(c, bench_type);
    }
}
