#![feature(custom_test_frameworks)]
#![test_runner(criterion::runner)]
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use criterion_macro::criterion;
use std::time::{Duration, SystemTime};

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

/// Sends a message marked with the send time to the receiver.
struct Sender {}

impl Sender {
    pub fn new(config: OperatorConfig<usize>, read_stream: ReadStream<()>, write_stream: WriteStream<(SystemTime, Vec<u8>)> -> Self {
        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<()>) -> WriteStream<(SystemTime, Vec<u8>)> {
        WriteStream::new()
    }
}

#[Criterion]
fn bench(c: &mut Criterion) {
    c.bench_function("custom iter test", |b| {
        b.iter_custom(|iters| black_box(Duration::from_secs(333)))
    });
    // c.bench_function("inter_thrd_msg_thrghput 100000 1", |b| {
    //     b.iter(|| inter_thread_msg_throughput(100000, 1))
    // });
    // c.bench_function("inter_thrd_msg_thrghput 1000 10000", |b| {
    //     b.iter(|| inter_thread_msg_throughput(1000, 10000))
    // });
    // c.bench_function("inter_proc_msg_thrghput 1000 1", |b| {
    //     b.iter(|| inter_process_msg_throughput(1000))
    // });
}

criterion_group!(benches, bench);
criterion_main!(benches);
