use std::{thread, time::Duration};

use erdos::{
    dataflow::{
        context::SinkContext,
        operator::{Sink, Source},
        stream::{ExtractStream, IngestStream, WriteStreamT},
        Message, Timestamp, WriteStream,
    },
    node::{WorkerHandle, WorkerId},
    Configuration, OperatorConfig,
};

struct SourceOperator {}

impl SourceOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Source<usize> for SourceOperator {
    fn run(&mut self, config: &OperatorConfig, write_stream: &mut WriteStream<usize>) {
        tracing::info!("Running {}", config.get_name());
        for t in 0..10 {
            let timestamp = Timestamp::Time(vec![t as u64]);
            write_stream
                .send(Message::new_message(timestamp.clone(), t))
                .unwrap();
            write_stream
                .send(Message::new_watermark(timestamp))
                .unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn destroy(&mut self) {
        tracing::info!("Destroying Source Operator");
    }
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<(), usize> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<()>, data: &usize) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received a data message with data: {}",
            ctx.operator_config().get_name(),
            timestamp,
            data
        );
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<()>) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received a watermark message.",
            ctx.operator_config().get_name(),
            timestamp,
        );
    }
}

fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let configuration = Configuration::from_args(&args);
    let worker_handle = WorkerHandle::new(configuration);

    // Construct the Graph.
    // let source_config = OperatorConfig::new().name("SourceOperator").node(0);
    // let source_stream = erdos::connect_source(SourceOperator::new, source_config);

    // let mut extract_stream = ExtractStream::new(&source_stream);

    let mut ingest_stream = IngestStream::new();

    let sink_config = OperatorConfig::new().name("SinkOperator").worker(WorkerId::from(0));
    erdos::connect_sink(SinkOperator::new, || {}, sink_config, &ingest_stream);

    // Submit the Graph.
    if worker_handle.id() == 0 {
        let _ = worker_handle.submit();
    } else {
        let _ = worker_handle.register();
    }

    // loop {
    //     match extract_stream.read() {
    //         Ok(message) => {
    //             println!("Received {:?} message.", message);
    //         }
    //         Err(error) => {}
    //     }
    // }

    let mut counter: usize = 0;
    while counter < 10 {
        if !ingest_stream.is_closed() {
            let timestamp = Timestamp::Time(vec![counter as u64]);
            let _ = ingest_stream.send(Message::new_message(timestamp, counter));
            counter += 1;
        }
    }
}
