extern crate erdos;

use std::{thread, time::Duration};

use erdos::dataflow::context::*;
use erdos::dataflow::deadlines::*;
use erdos::dataflow::operator::*;
use erdos::dataflow::operators::FilterOperator;
use erdos::dataflow::operators::FlatMapOperator;
use erdos::dataflow::operators::SplitOperator;
use erdos::dataflow::state::TimeVersionedState;
use erdos::dataflow::stream::*;
use erdos::dataflow::Graph;
use erdos::dataflow::*;
use erdos::Configuration;

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
            // thread::sleep(Duration::new(5, 0));
        }
    }

    fn destroy(&mut self) {
        tracing::info!("Destroying Source Operator");
    }
}

struct SquareOperator {}

impl SquareOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl OneInOneOut<(), usize, usize> for SquareOperator {
    fn setup(&mut self, ctx: &mut SetupContext<()>) {
        ctx.add_deadline(TimestampDeadline::new(
            move |_s: &(), _t: &Timestamp| -> Duration { Duration::new(2, 0) },
            |_s: &(), _t: &Timestamp| {
                tracing::info!("SquareOperator @ {:?}: Missed deadline.", _t);
            },
        ));
    }

    fn on_data(&mut self, ctx: &mut OneInOneOutContext<(), usize>, data: &usize) {
        thread::sleep(Duration::new(2, 0));
        tracing::info!("SquareOperator @ {:?}: received {}", ctx.timestamp(), data);
        let timestamp = ctx.timestamp().clone();
        ctx.write_stream()
            .send(Message::new_message(timestamp, data * data))
            .unwrap();
        tracing::info!(
            "SquareOperator @ {:?}: sent {}",
            ctx.timestamp(),
            data * data
        );
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<(), usize>) {}
}

struct SumOperator {}

#[allow(dead_code)]
impl SumOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl OneInOneOut<TimeVersionedState<usize>, usize, usize> for SumOperator {
    fn on_data(
        &mut self,
        ctx: &mut OneInOneOutContext<TimeVersionedState<usize>, usize>,
        data: &usize,
    ) {
        tracing::info!("SumOperator @ {:?}: Received {}", ctx.timestamp(), data);

        let timestamp = ctx.timestamp().clone();
        let timestamp_clone = ctx.timestamp().clone();

        // Find the last committed state, add the received data to it, and save for this timestamp.
        {
            let past_state = ctx.past_state(&ctx.last_committed_timestamp()).unwrap();
            *ctx.current_state().unwrap() += past_state + data;
        }

        // Send the message.
        let current_state_copy = *ctx.current_state().unwrap();
        ctx.write_stream()
            .send(Message::new_message(timestamp, current_state_copy))
            .unwrap();
        tracing::info!(
            "SumOperator @ {:?}: Sent {}",
            timestamp_clone,
            current_state_copy
        );
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<TimeVersionedState<usize>, usize>) {}
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<TimeVersionedState<usize>, usize> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>, data: &usize) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received {}",
            ctx.operator_config().get_name(),
            timestamp,
            data
        );
        *ctx.current_state().unwrap() += 1;
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received {} data messages.",
            ctx.operator_config().get_name(),
            timestamp,
            ctx.current_state().unwrap(),
        );
    }
}

struct JoinSumOperator {}

#[allow(dead_code)]
impl JoinSumOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl TwoInOneOut<TimeVersionedState<usize>, usize, usize, usize> for JoinSumOperator {
    fn on_left_data(
        &mut self,
        ctx: &mut TwoInOneOutContext<TimeVersionedState<usize>, usize>,
        data: &usize,
    ) {
        let current_timestamp = ctx.timestamp().clone();
        let current_state = ctx.current_state().unwrap();
        *current_state += *data;

        tracing::info!(
            "JoinSumOperator @ {:?}: Received {} on left stream, sum is {}",
            current_timestamp,
            data,
            *current_state
        );
    }

    fn on_right_data(
        &mut self,
        ctx: &mut TwoInOneOutContext<TimeVersionedState<usize>, usize>,
        data: &usize,
    ) {
        let current_timestamp = ctx.timestamp().clone();
        let current_state = ctx.current_state().unwrap();
        *current_state += *data;

        tracing::info!(
            "JoinSumOperator @ {:?}: Received {} on right stream, sum is {}",
            current_timestamp,
            data,
            *current_state
        );
    }

    fn on_watermark(&mut self, ctx: &mut TwoInOneOutContext<TimeVersionedState<usize>, usize>) {
        let state_copy = *ctx.current_state().unwrap();
        let time = ctx.timestamp().clone();
        tracing::info!(
            "JoinSumOperator @ {:?}: received watermark, sending sum of {}",
            time,
            state_copy,
        );
        ctx.write_stream()
            .send(Message::new_message(time, state_copy))
            .unwrap();
    }
}

struct EvenOddOperator {}

#[allow(dead_code)]
impl EvenOddOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl OneInTwoOut<(), usize, usize, usize> for EvenOddOperator {
    fn on_data(&mut self, ctx: &mut OneInTwoOutContext<(), usize, usize>, data: &usize) {
        let time = ctx.timestamp().clone();
        if data % 2 == 0 {
            tracing::info!(
                "EvenOddOperator @ {:?}: sending even number {} on left stream",
                ctx.timestamp(),
                data,
            );
            ctx.left_write_stream()
                .send(Message::new_message(time, *data))
                .unwrap();
        } else {
            tracing::info!(
                "EvenOddOperator @ {:?}: sending odd number {} on right stream",
                ctx.timestamp(),
                data,
            );
            ctx.right_write_stream()
                .send(Message::new_message(time, *data))
                .unwrap();
        }
    }

    fn on_watermark(&mut self, _ctx: &mut OneInTwoOutContext<(), usize, usize>) {}
}

fn main() {
    //let mut s = TimestampDeadline::new().with_start_condition(45);
    //println!("The s value is {}", s.s);
    let args = erdos::new_app("ERDOS").get_matches();

    // let mut node = Node::new(Configuration::from_args(&args));
    let graph = Graph::new();

    let source_config = OperatorConfig::new().name("SourceOperator");
    let source_stream = graph.connect_source(SourceOperator::new, source_config);

    let square_config = OperatorConfig::new().name("SquareOperator");
    let square_stream =
        graph.connect_one_in_one_out(SquareOperator::new, || {}, square_config, &source_stream);

    let map_config = OperatorConfig::new().name("FlatMapOperator");
    let map_stream = graph.connect_one_in_one_out(
        || -> FlatMapOperator<usize, _> {
            FlatMapOperator::new(|x: &usize| std::iter::once(2 * x))
        },
        || {},
        map_config,
        &square_stream,
    );

    let filter_config = OperatorConfig::new().name("FilterOperator");
    let filter_stream = graph.connect_one_in_one_out(
        || -> FilterOperator<usize> { FilterOperator::new(|x: &usize| -> bool { *x > 10 }) },
        || {},
        filter_config,
        &map_stream,
    );

    let split_config = OperatorConfig::new().name("SplitOperator");
    let (split_stream_less_50, split_stream_greater_50) = graph.connect_one_in_two_out(
        || -> SplitOperator<usize> { SplitOperator::new(|x: &usize| -> bool { *x < 50 }) },
        || {},
        split_config,
        &filter_stream,
    );

    let sum_config = OperatorConfig::new().name("SumOperator");
    let sum_stream = graph.connect_one_in_one_out(
        SumOperator::new,
        TimeVersionedState::new,
        sum_config,
        &square_stream,
    );

    let left_sink_config = OperatorConfig::new().name("LeftSinkOperator");
    graph.connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        left_sink_config,
        &split_stream_less_50,
    );

    let right_sink_config = OperatorConfig::new().name("RightSinkOperator");
    graph.connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        right_sink_config,
        &split_stream_greater_50,
    );

    // Example use of an ingress stream.
    let ingress_stream: IngressStream<usize> = graph.add_ingress("Ingest1");
    let sink_config = OperatorConfig::new().name("IngestSinkOperator");
    graph.connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        sink_config,
        &ingress_stream,
    );

    let join_sum_config = OperatorConfig::new().name("JoinSumOperator");
    let _join_stream = graph.connect_two_in_one_out(
        JoinSumOperator::new,
        TimeVersionedState::new,
        join_sum_config,
        &source_stream,
        &sum_stream,
    );

    let even_odd_config = OperatorConfig::new().name("EvenOddOperator");
    let (_even_stream, _odd_stream) =
        graph.connect_one_in_two_out(EvenOddOperator::new, || {}, even_odd_config, &source_stream);

    let mut loop_stream: LoopStream<usize> = graph.add_loop_stream();
    loop_stream.connect_loop(&source_stream);

    let _egress_stream: EgressStream<usize> = square_stream.to_egress();

    // node.run(graph);
}
