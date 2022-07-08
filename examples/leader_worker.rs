use std::{thread, time::Duration};

use erdos::{
    dataflow::{
        context::SinkContext,
        operator::{Sink, Source},
        stream::WriteStreamT,
        Graph, Message, Timestamp, WriteStream,
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
    let mut worker_handle = WorkerHandle::new(configuration);

    // Construct the Graph.
    let graph = Graph::new("LeaderWorkerExample");
    // let source_config = OperatorConfig::new().name("SourceOperator").worker(WorkerId::from(0));
    // let source_stream = graph.connect_source(SourceOperator::new, source_config);

    // let mut extract_stream = ExtractStream::new(&source_stream);
    // let mut extract_stream = source_stream.to_egress();

    let mut ingress_stream = graph.add_ingress("IngressStream");

    let sink_config = OperatorConfig::new()
        .name("SinkOperator")
        .worker(WorkerId::from(1));
    graph.connect_sink(SinkOperator::new, || {}, sink_config, &ingress_stream);

    // Submit the Graph.
    if worker_handle.id() == WorkerId::from(0) {
        if let Ok(graph_id) = worker_handle.submit(graph) {
            loop {
                match worker_handle.ready(&graph_id) {
                    Ok(status) => {
                        if status {
                            break;
                        }
                    }
                    Err(_) => {}
                }
                std::thread::sleep_ms(2000);
            }
            println!("The graph {:?} is ready.", graph_id);

            let mut counter: usize = 0;
            while counter < 10 {
                let timestamp = Timestamp::Time(vec![counter as u64]);
                let _ = ingress_stream.send(Message::new_message(timestamp, counter));
                counter += 1;
                std::thread::sleep_ms(1000);
            }
        } else {
            return;
        }
    } else {
        if let Ok(_) = worker_handle.register(graph) {
            loop {}
        }
    };

    // Wait for the JobGraph to be setup.

    // loop {
    //     match extract_stream.read() {
    //         Ok(message) => {
    //             println!("Received {:?} message.", message);
    //         }
    //         Err(error) => {}
    //     }
    // }
}
