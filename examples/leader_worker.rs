use std::{time::Duration, thread};

use erdos::{node::handles::WorkerHandle, Configuration, dataflow::{operator::{Source, Sink}, WriteStream, Timestamp, Message, stream::WriteStreamT, context::SinkContext}, OperatorConfig};


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
    let worker_handle = WorkerHandle::new(Configuration::from_args(&args));
    loop {}
}
