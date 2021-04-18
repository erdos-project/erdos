#[macro_use]
extern crate erdos;

use std::{sync::Arc, thread, time::Duration};

use erdos::dataflow::deadlines::*;
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

struct SquareOperatorDeadlineContext {}

impl SquareOperatorDeadlineContext {
    pub fn new() -> Self {
        Self {}
    }
}

impl DeadlineContext for SquareOperatorDeadlineContext {
    fn calculate_deadline(&self, ctx: &ConditionContext) -> Duration {
        Duration::new(1, 0)
    }
}

struct SquareOperatorHandlerContext {}

impl SquareOperatorHandlerContext {
    pub fn new() -> Self {
        Self {}
    }
}

impl HandlerContextT for SquareOperatorHandlerContext {
    fn invoke_handler(&mut self, ctx: &ConditionContext, current_timestamp: &Timestamp) {
        println!("Handled {:?} errors.", ctx);
    }
}

impl OneInOneOut<(), usize, usize> for SquareOperator {
    fn setup(&mut self, ctx: &mut OneInOneOutSetupContext) {
        println!("Executed setup!");
        let deadline = TimestampDeadline::new(
            SquareOperatorDeadlineContext::new(),
            SquareOperatorHandlerContext::new(),
        )
        .on_read_stream(ctx.read_stream_id);
        ctx.add_deadline(Deadline::TimestampDeadline(deadline));
    }

    fn on_data(ctx: &mut OneInOneOutContext<usize>, data: &usize) {
        thread::sleep(Duration::new(2, 0));
        let logger = erdos::get_terminal_logger();
        slog::info!(
            logger,
            "SquareOperator @ {:?}: received {}",
            ctx.timestamp,
            data
        );
        ctx.write_stream
            .send(Message::new_message(ctx.timestamp.clone(), data * data))
            .unwrap();
        slog::info!(
            logger,
            "SquareOperator @ {:?}: sent {}",
            ctx.timestamp,
            data * data
        );
    }

    fn on_data_stateful(ctx: &mut StatefulOneInOneOutContext<(), usize>, data: &usize) {}

    fn on_watermark(ctx: &mut StatefulOneInOneOutContext<(), usize>) {}
}

struct SumOperator {}

impl SumOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl OneInOneOut<usize, usize, usize> for SumOperator {
    fn on_data(ctx: &mut OneInOneOutContext<usize>, data: &usize) {}

    // Proof of concept that state "works", although it is mis-implemented.
    // Also need to fix the lattice.
    fn on_data_stateful(ctx: &mut StatefulOneInOneOutContext<usize, usize>, data: &usize) {
        let logger = erdos::get_terminal_logger();
        slog::info!(
            logger,
            "SumOperator @ {:?}: received {}",
            ctx.timestamp,
            data
        );
        let mut state = ctx.state.try_lock().unwrap();
        *state += data;
        ctx.write_stream
            .send(Message::new_message(ctx.timestamp.clone(), *state))
            .unwrap();
        slog::info!(logger, "SumOperator @ {:?}: sent {}", ctx.timestamp, state);
    }

    fn on_watermark(ctx: &mut StatefulOneInOneOutContext<usize, usize>) {}
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<(usize, usize), usize> for SinkOperator {
    fn on_data(ctx: &mut SinkContext, data: &usize) {
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: received {}",
            ctx.timestamp,
            data
        )
    }

    fn on_data_stateful(ctx: &mut StatefulSinkContext<(usize, usize)>, _data: &usize) {
        // State := number of messages and watermarks received.
        let mut state = ctx.state.try_lock().unwrap();
        state.0 += 1;
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: received {} data messages, {} watermarks",
            ctx.timestamp,
            state.0,
            state.1,
        )
    }

    fn on_watermark(ctx: &mut StatefulSinkContext<(usize, usize)>) {
        // State := number of messages and watermarks received.
        let mut state = ctx.state.try_lock().unwrap();
        state.1 += 1;
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: received {} data messages, {} watermarks",
            ctx.timestamp,
            state.0,
            state.1,
        )
    }
}

struct JoinSumOperator {}

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

    fn on_data_stateful(ctx: &mut StatefulOneInTwoOutContext<(), usize, usize>, data: &usize) {}

    fn on_watermark(ctx: &mut StatefulOneInTwoOutContext<(), usize, usize>) {}
}

fn main() {
    //let mut s = TimestampDeadline::new().with_start_condition(45);
    //println!("The s value is {}", s.s);
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let source_config = OperatorConfig::new().name("SourceOperator");
    let source_stream = erdos::connect_source(SourceOperator::new, || {}, source_config);

    let square_config = OperatorConfig::new().name("SquareOperator");
    let square_stream =
        erdos::connect_one_in_one_out(SquareOperator::new, || {}, square_config, &source_stream);

    //let sum_config = OperatorConfig::new().name("SumOperator");
    //let sum_stream =
    //    erdos::connect_one_in_one_out(SumOperator::new, || 0, sum_config, &square_stream);

    let sink_config = OperatorConfig::new().name("SinkOperator");
    erdos::connect_sink(SinkOperator::new, || (0, 0), sink_config, &square_stream);

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
