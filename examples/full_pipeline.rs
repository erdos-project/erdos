extern crate erdos;

use std::{collections::HashMap, thread, time::Duration};

use erdos::dataflow::context::*;
use erdos::dataflow::deadlines::*;
use erdos::dataflow::operator::*;
use erdos::dataflow::operators::*;
use erdos::dataflow::stream::IngestStream;
use erdos::dataflow::stream::WriteStreamT;
use erdos::dataflow::*;
use erdos::node::Node;
use erdos::Configuration;

struct SourceOperator {}

impl SourceOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Source<(), usize> for SourceOperator {
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

struct SquareOperatorState {
    current_timestamp: Timestamp,
}

impl SquareOperatorState {
    fn new() -> Self {
        Self {
            current_timestamp: Timestamp::Bottom,
        }
    }
}

impl StateT for SquareOperatorState {
    fn commit(&mut self, timestamp: &Timestamp) {
        self.current_timestamp = timestamp.clone();
    }

    fn get_last_committed_timestamp(&self) -> Timestamp {
        self.current_timestamp.clone()
    }
}

impl OneInOneOut<SquareOperatorState, usize, usize> for SquareOperator {
    fn setup(&mut self, ctx: &mut SetupContext<SquareOperatorState>) {
        ctx.add_deadline(TimestampDeadline::new(
            move |_s: &SquareOperatorState, _t: &Timestamp| -> Duration { Duration::new(2, 0) },
            |_s: &SquareOperatorState, _t: &Timestamp| {
                tracing::info!("SquareOperator @ {:?}: Missed deadline.", _t);
            },
        ));
    }

    fn on_data(&mut self, ctx: &mut OneInOneOutContext<SquareOperatorState, usize>, data: &usize) {
        thread::sleep(Duration::new(2, 0));
        tracing::info!(
            "SquareOperator @ {:?}: received {}",
            ctx.get_timestamp(),
            data
        );
        let timestamp = ctx.get_timestamp().clone();
        ctx.get_write_stream()
            .send(Message::new_message(timestamp, data * data))
            .unwrap();
        tracing::info!(
            "SquareOperator @ {:?}: sent {}",
            ctx.get_timestamp(),
            data * data
        );
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<SquareOperatorState, usize>) {}
}

struct SumOperator {}

#[allow(dead_code)]
impl SumOperator {
    pub fn new() -> Self {
        Self {}
    }
}

struct SumOperatorState {
    counter: usize,
    current_timestamp: Timestamp,
}

#[allow(dead_code)]
impl SumOperatorState {
    fn new() -> Self {
        Self {
            counter: 0,
            current_timestamp: Timestamp::Bottom,
        }
    }

    fn increment_counter(&mut self, value: usize) {
        self.counter += value;
    }

    fn get_counter(&self) -> usize {
        self.counter
    }
}

impl StateT for SumOperatorState {
    fn commit(&mut self, timestamp: &Timestamp) {
        self.current_timestamp = timestamp.clone();
    }

    fn get_last_committed_timestamp(&self) -> Timestamp {
        self.current_timestamp.clone()
    }
}

impl OneInOneOut<SumOperatorState, usize, usize> for SumOperator {
    fn on_data(&mut self, ctx: &mut OneInOneOutContext<SumOperatorState, usize>, data: &usize) {
        tracing::info!("SumOperator @ {:?}: Received {}", ctx.get_timestamp(), data);

        let timestamp = ctx.get_timestamp().clone();
        ctx.get_state().increment_counter(*data);
        let state = ctx.get_state().get_counter();
        ctx.get_write_stream()
            .send(Message::new_message(timestamp, state))
            .unwrap();
        tracing::info!("SumOperator @ {:?}: Sent {}", ctx.get_timestamp(), state);
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<SumOperatorState, usize>) {}
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

struct SinkOperatorState {
    message_counter: HashMap<Timestamp, usize>,
    current_timestamp: Timestamp,
}

impl SinkOperatorState {
    fn new() -> Self {
        Self {
            message_counter: HashMap::new(),
            current_timestamp: Timestamp::Bottom,
        }
    }

    fn increment_message_count(&mut self, timestamp: &Timestamp) {
        let count = self.message_counter.entry(timestamp.clone()).or_insert(0);
        *count += 1;
    }

    fn get_message_count(&self, timestamp: &Timestamp) -> usize {
        *self.message_counter.get(timestamp).unwrap_or_else(|| &0)
    }
}

impl StateT for SinkOperatorState {
    fn commit(&mut self, timestamp: &Timestamp) {
        self.current_timestamp = timestamp.clone();
    }

    fn get_last_committed_timestamp(&self) -> Timestamp {
        self.current_timestamp.clone()
    }
}

impl Sink<SinkOperatorState, usize> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<SinkOperatorState>, data: &usize) {
        let timestamp = ctx.get_timestamp().clone();
        tracing::info!("SinkOperator @ {:?}: Received {}", timestamp, data);
        ctx.get_state().increment_message_count(&timestamp);
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<SinkOperatorState>) {
        let timestamp = ctx.get_timestamp().clone();
        tracing::info!(
            "SinkOperator @ {:?}: Received {} data messages.",
            timestamp,
            ctx.get_state().get_message_count(&timestamp),
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

struct JoinSumOperatorState {
    sum: usize,
    current_timestamp: Timestamp,
}

#[allow(dead_code)]
impl JoinSumOperatorState {
    fn new() -> Self {
        Self {
            sum: 0,
            current_timestamp: Timestamp::Bottom,
        }
    }

    fn add(&mut self, value: usize) {
        self.sum += value;
    }

    fn get_sum(&self) -> usize {
        self.sum
    }
}

impl StateT for JoinSumOperatorState {
    fn commit(&mut self, timestamp: &Timestamp) {
        self.current_timestamp = timestamp.clone();
    }

    fn get_last_committed_timestamp(&self) -> Timestamp {
        self.current_timestamp.clone()
    }
}

impl TwoInOneOut<JoinSumOperatorState, usize, usize, usize> for JoinSumOperator {
    fn on_left_data(
        &mut self,
        ctx: &mut TwoInOneOutContext<JoinSumOperatorState, usize>,
        data: &usize,
    ) {
        ctx.get_state().add(*data);
        let state = ctx.get_state().get_sum();

        tracing::info!(
            "JoinSumOperator @ {:?}: Received {} on left stream, sum is {}",
            ctx.get_timestamp(),
            data,
            state
        );
    }

    fn on_right_data(
        &mut self,
        ctx: &mut TwoInOneOutContext<JoinSumOperatorState, usize>,
        data: &usize,
    ) {
        ctx.get_state().add(*data);
        let state = ctx.get_state().get_sum();

        tracing::info!(
            "JoinSumOperator @ {:?}: Received {} on right stream, sum is {}",
            ctx.get_timestamp(),
            data,
            state
        );
    }

    fn on_watermark(&mut self, ctx: &mut TwoInOneOutContext<JoinSumOperatorState, usize>) {
        let state = ctx.get_state().get_sum();
        let time = ctx.get_timestamp().clone();
        tracing::info!(
            "JoinSumOperator @ {:?}: received watermark, sending sum of {}",
            time,
            state,
        );
        ctx.get_write_stream()
            .send(Message::new_message(time, state))
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

struct EvenOddOperatorState {
    current_timestamp: Timestamp,
}

#[allow(dead_code)]
impl EvenOddOperatorState {
    fn new() -> Self {
        Self {
            current_timestamp: Timestamp::Bottom,
        }
    }
}

impl StateT for EvenOddOperatorState {
    fn commit(&mut self, timestamp: &Timestamp) {
        self.current_timestamp = timestamp.clone();
    }

    fn get_last_committed_timestamp(&self) -> Timestamp {
        self.current_timestamp.clone()
    }
}

impl OneInTwoOut<EvenOddOperatorState, usize, usize, usize> for EvenOddOperator {
    fn on_data(
        &mut self,
        ctx: &mut OneInTwoOutContext<EvenOddOperatorState, usize, usize>,
        data: &usize,
    ) {
        let time = ctx.get_timestamp().clone();
        if data % 2 == 0 {
            tracing::info!(
                "EvenOddOperator @ {:?}: sending even number {} on left stream",
                ctx.get_timestamp(),
                data,
            );
            ctx.get_left_write_stream()
                .send(Message::new_message(time, *data))
                .unwrap();
        } else {
            tracing::info!(
                "EvenOddOperator @ {:?}: sending odd number {} on right stream",
                ctx.get_timestamp(),
                data,
            );
            ctx.get_right_write_stream()
                .send(Message::new_message(time, *data))
                .unwrap();
        }
    }

    fn on_watermark(&mut self, _ctx: &mut OneInTwoOutContext<EvenOddOperatorState, usize, usize>) {}
}

fn main() {
    //let mut s = TimestampDeadline::new().with_start_condition(45);
    //println!("The s value is {}", s.s);
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let source_config = OperatorConfig::new().name("SourceOperator");
    let source_stream = erdos::connect_source(SourceOperator::new, || {}, source_config);

    let square_config = OperatorConfig::new().name("SquareOperator");
    let square_stream = erdos::connect_one_in_one_out(
        SquareOperator::new,
        SquareOperatorState::new,
        square_config,
        &source_stream,
    );

    let map_config = OperatorConfig::new().name("FlatMapOperator");
    let map_stream = erdos::connect_one_in_one_out(
        || -> FlatMapOperator<usize, _> {
            FlatMapOperator::new(|a: &usize| std::iter::once(2 * a))
        },
        || {},
        map_config,
        &square_stream,
    );

    let filter_config = OperatorConfig::new().name("FilterOperator");
    let filter_stream = erdos::connect_one_in_one_out(
        || -> FilterOperator<usize> { FilterOperator::new(|a: &usize| -> bool { a > &10 }) },
        || {},
        filter_config,
        &map_stream,
    );

    let split_config = OperatorConfig::new().name("SplitOperator");
    let (split_stream_less_50, split_stream_greater_50) = erdos::connect_one_in_two_out(
        || -> SplitOperator<usize> { SplitOperator::new(|a: &usize| -> bool { a < &50 }) },
        || {},
        split_config,
        &filter_stream,
    );

    //let sum_config = OperatorConfig::new().name("SumOperator");
    //let sum_stream = erdos::connect_one_in_one_out(
    //    SumOperator::new,
    //    SumOperatorState::new,
    //    sum_config,
    //    &square_stream,
    //);

    let left_sink_config = OperatorConfig::new().name("LeftSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        SinkOperatorState::new,
        left_sink_config,
        &split_stream_less_50,
    );

    let right_sink_config = OperatorConfig::new().name("RightSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        SinkOperatorState::new,
        right_sink_config,
        &split_stream_greater_50,
    );

    // Example use of an ingest stream.
    let ingest_stream = IngestStream::new();
    let sink_config = OperatorConfig::new().name("IngestSinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        SinkOperatorState::new,
        sink_config,
        &ingest_stream,
    );

    // let join_sum_config = OperatorConfig::new().name("JoinSumOperator");
    // let join_stream = erdos::connect_two_in_one_out(
    //    JoinSumOperator::new,
    //    JoinSumOperatorState::new,
    //    join_sum_config,
    //    &source_stream,
    //    &sum_stream,
    // );

    //let even_odd_config = OperatorConfig::new().name("EvenOddOperator");
    //let (even_stream, odd_stream) =
    //    erdos::connect_one_in_two_out(EvenOddOperator::new, || {}, even_odd_config, &source_stream);

    node.run();
}
