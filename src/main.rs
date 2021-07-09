extern crate erdos;

use std::{collections::HashMap, thread, time::Duration};

use erdos::dataflow::operator::*;
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
    fn run(&mut self, write_stream: &mut WriteStream<usize>) {
        let logger = erdos::get_terminal_logger();
        slog::info!(logger, "Running Source Operator");
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
        slog::info!(erdos::get_terminal_logger(), "Destroying Source Operator");
    }
}

struct SquareOperator {}

impl SquareOperator {
    pub fn new() -> Self {
        Self {}
    }
}

struct SquareOperatorState {}

impl SquareOperatorState {
    fn new() -> Self {
        Self {}
    }
}

impl WriteableState<()> for SquareOperatorState {
    fn commit(&mut self, _state: &(), _timestamp: &Timestamp) {}
}

impl OneInOneOut<SquareOperatorState, usize, usize, ()> for SquareOperator {
    fn on_data(
        &mut self,
        ctx: &mut OneInOneOutContext<SquareOperatorState, usize, ()>,
        data: &usize,
    ) {
        thread::sleep(Duration::new(2, 0));
        let logger = erdos::get_terminal_logger();
        slog::info!(
            logger,
            "SquareOperator @ {:?}: received {}",
            ctx.get_timestamp(),
            data
        );
        let timestamp = ctx.get_timestamp().clone();
        ctx.get_write_stream()
            .send(Message::new_message(timestamp, data * data))
            .unwrap();
        slog::info!(
            logger,
            "SquareOperator @ {:?}: sent {}",
            ctx.get_timestamp(),
            data * data
        );
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<SquareOperatorState, usize, ()>) {}
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
}

#[allow(dead_code)]
impl SumOperatorState {
    fn new() -> Self {
        Self { counter: 0 }
    }

    fn increment_counter(&mut self, value: usize) {
        self.counter += value;
    }

    fn get_counter(&self) -> usize {
        self.counter
    }
}

impl WriteableState<usize> for SumOperatorState {
    fn commit(&mut self, _state: &usize, _timestamp: &Timestamp) {}
}

impl OneInOneOut<SumOperatorState, usize, usize, usize> for SumOperator {
    fn on_data(
        &mut self,
        ctx: &mut OneInOneOutContext<SumOperatorState, usize, usize>,
        data: &usize,
    ) {
        slog::info!(
            erdos::get_terminal_logger(),
            "SumOperator @ {:?}: Received {}",
            ctx.get_timestamp(),
            data
        );

        let timestamp = ctx.get_timestamp().clone();
        ctx.get_state().increment_counter(*data);
        let state = ctx.get_state().get_counter();
        ctx.get_write_stream()
            .send(Message::new_message(timestamp, state))
            .unwrap();
        slog::info!(
            erdos::get_terminal_logger(),
            "SumOperator @ {:?}: Sent {}",
            ctx.get_timestamp(),
            state
        );
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<SumOperatorState, usize, usize>) {}
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

struct SinkOperatorState {
    message_counter: HashMap<Timestamp, usize>,
}

impl SinkOperatorState {
    fn new() -> Self {
        Self {
            message_counter: HashMap::new(),
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

impl WriteableState<usize> for SinkOperatorState {
    fn commit(&mut self, _state: &usize, _timestamp: &Timestamp) {}
}

impl Sink<SinkOperatorState, usize, usize> for SinkOperator {
    fn on_data(&mut self, ctx: &mut SinkContext<SinkOperatorState, usize>, data: &usize) {
        let timestamp = ctx.get_timestamp().clone();
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: Received {}",
            timestamp,
            data
        );
        ctx.get_state().increment_message_count(&timestamp);
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<SinkOperatorState, usize>) {
        let timestamp = ctx.get_timestamp().clone();
        slog::info!(
            erdos::get_terminal_logger(),
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

impl TwoInOneOut<usize, usize, usize, usize> for JoinSumOperator {
    fn on_left_data(_ctx: &mut TwoInOneOutContext<usize>, _data: &usize) {}

    fn on_left_data_stateful(ctx: &mut StatefulTwoInOneOutContext<usize, usize>, data: &usize) {
        let mut state = ctx.state.try_lock().unwrap();
        *state += data;
        slog::info!(
            erdos::get_terminal_logger(),
            "JoinSumOperator @ {:?}: received {} on left stream, sum is {}",
            ctx.timestamp,
            data,
            state,
        );
    }

    fn on_right_data(_ctx: &mut TwoInOneOutContext<usize>, _data: &usize) {}

    fn on_right_data_stateful(ctx: &mut StatefulTwoInOneOutContext<usize, usize>, data: &usize) {
        let mut state = ctx.state.try_lock().unwrap();
        *state += data;
        slog::info!(
            erdos::get_terminal_logger(),
            "JoinSumOperator @ {:?}: received {} on right stream, sum is {}",
            ctx.timestamp,
            data,
            state,
        );
    }

    fn on_watermark(ctx: &mut StatefulTwoInOneOutContext<usize, usize>) {
        let state = ctx.state.try_lock().unwrap();
        slog::info!(
            erdos::get_terminal_logger(),
            "JoinSumOperator @ {:?}: received watermark, sending sum of {}",
            ctx.timestamp,
            state,
        );
        ctx.write_stream
            .send(Message::new_message(ctx.timestamp.clone(), *state))
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
    fn on_data(ctx: &mut OneInTwoOutContext<usize, usize>, data: &usize) {
        if data % 2 == 0 {
            slog::info!(
                erdos::get_terminal_logger(),
                "EvenOddOperator @ {:?}: sending even number {} on left stream",
                ctx.timestamp,
                data,
            );
            ctx.left_write_stream
                .send(Message::new_message(ctx.timestamp.clone(), *data))
                .unwrap();
        } else {
            slog::info!(
                erdos::get_terminal_logger(),
                "EvenOddOperator @ {:?}: sending odd number {} on right stream",
                ctx.timestamp,
                data,
            );
            ctx.right_write_stream
                .send(Message::new_message(ctx.timestamp.clone(), *data))
                .unwrap();
        }
    }

    fn on_data_stateful(_ctx: &mut StatefulOneInTwoOutContext<(), usize, usize>, _data: &usize) {}

    fn on_watermark(_ctx: &mut StatefulOneInTwoOutContext<(), usize, usize>) {}
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

    //let sum_config = OperatorConfig::new().name("SumOperator");
    //let sum_stream = erdos::connect_one_in_one_out(
    //    SumOperator::new,
    //    SumOperatorState::new,
    //    sum_config,
    //    &square_stream,
    //);

    let sink_config = OperatorConfig::new().name("SinkOperator");
    erdos::connect_sink(
        SinkOperator::new,
        SinkOperatorState::new,
        sink_config,
        &square_stream,
    );

    //let join_sum_config = OperatorConfig::new().name("JoinSumOperator");
    //let join_stream = erdos::connect_two_in_one_out(
    //    JoinSumOperator::new,
    //    || 0,
    //    join_sum_config,
    //    &source_stream,
    //    &sum_stream,
    //);

    //let even_odd_config = OperatorConfig::new().name("EvenOddOperator");
    //let (even_stream, odd_stream) =
    //    erdos::connect_one_in_two_out(EvenOddOperator::new, || {}, even_odd_config, &source_stream);

    node.run();
}
