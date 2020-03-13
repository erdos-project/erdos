use std::thread;
use std::time::Duration;

use slog;

use erdos::{
    self,
    dataflow::{
        message::*,
        state::{State, TimeVersionedState},
        stream::{
            errors::{ReadError, TryReadError, WriteStreamError},
            ExtractStream, IngestStream, WriteStreamT,
        },
        Operator, OperatorConfig, ReadStream, WriteStream,
    },
    node::Node,
    *,
};

/// Sends 3 data messages, 1 watermark message per timestamp in incrementing order.
struct SendOp {
    write_stream: WriteStream<usize>,
}

impl SendOp {
    pub fn new(_config: OperatorConfig<()>, write_stream: WriteStream<usize>) -> Self {
        Self { write_stream }
    }

    pub fn connect() -> WriteStream<usize> {
        WriteStream::new()
    }
}

impl Operator for SendOp {
    fn run(&mut self) {
        let logger = erdos::get_terminal_logger();
        for i in 0.. {
            let timestamp = Timestamp::new(vec![i as u64]);
            for j in 0..3 {
                let msg = Message::new_message(timestamp.clone(), i + j);
                self.write_stream.send(msg).unwrap();
            }
            let watermark = Message::new_watermark(timestamp);
            self.write_stream.send(watermark).unwrap();
        }
    }
}

struct TimeVersionedStateOp {}

impl TimeVersionedStateOp {
    pub fn new(
        _config: OperatorConfig<()>,
        read_stream: ReadStream<usize>,
        write_stream: WriteStream<usize>,
    ) -> Self {
        // TODO: improve on the API for joining streams.
        let cb_state = TimeVersionedState::<(), usize>::new();
        let mut watermark_state = TimeVersionedState::<usize, ()>::new_with_history_size(1);
        watermark_state.set_initial_state(100);
        let mut stateful_read_stream = read_stream.add_state(cb_state);
        stateful_read_stream.add_callback(Self::callback);
        stateful_read_stream
            .add_write_stream(&write_stream)
            .borrow_mut()
            .add_state(watermark_state)
            .borrow_mut()
            .add_watermark_callback(Self::watermark_callback);

        Self {}
    }

    pub fn connect(read_stream: &ReadStream<usize>) -> WriteStream<usize> {
        WriteStream::new()
    }

    pub fn callback(_t: Timestamp, data: usize, cb_state: &mut TimeVersionedState<(), usize>) {
        cb_state.append(data);
    }

    pub fn watermark_callback(
        _t: &Timestamp,
        watermark_state: &mut TimeVersionedState<usize, ()>,
        cb_state: &TimeVersionedState<(), usize>,
        write_stream: &mut WriteStream<usize>,
    ) {
    }
}
