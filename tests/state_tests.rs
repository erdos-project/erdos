use std::thread;
use std::time::Duration;

use slog;

mod utils;

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

/// Sums all messages for a given timestamp and subtracts the previous state.
/// Sends None on error.
struct TimeVersionedStateOp {}

impl TimeVersionedStateOp {
    pub fn new(
        _config: OperatorConfig<()>,
        read_stream: ReadStream<usize>,
        write_stream: WriteStream<Option<usize>>,
    ) -> Self {
        // TODO: improve on the API for joining streams.
        let cb_state = TimeVersionedState::<(), usize>::new();
        let mut watermark_state = TimeVersionedState::<usize, ()>::new_with_history_size(1);
        watermark_state.set_initial_state(0).unwrap();
        let stateful_read_stream = read_stream.add_state(cb_state);
        stateful_read_stream.add_callback(Self::callback);
        stateful_read_stream
            .add_write_stream(&write_stream)
            .borrow_mut()
            .add_state(watermark_state)
            .borrow_mut()
            .add_watermark_callback(Self::watermark_callback);

        Self {}
    }

    pub fn connect(read_stream: &ReadStream<usize>) -> WriteStream<Option<usize>> {
        WriteStream::new()
    }

    pub fn callback(_t: Timestamp, data: usize, cb_state: &mut TimeVersionedState<(), usize>) {
        cb_state.append(data);
    }

    fn watermark_callback_helper(
        t: &Timestamp,
        watermark_state: &mut TimeVersionedState<usize, ()>,
        cb_state: &TimeVersionedState<(), usize>,
    ) -> Option<usize> {
        let sum: usize = cb_state.get_current_messages().ok()?.iter().sum();
        let previous_state = *watermark_state.iter_states().ok()?.nth(1)?.1;
        let new_state = sum - previous_state;
        *watermark_state.get_current_state_mut().ok()? = new_state;
        watermark_state.close_time(t).ok();
        Some(new_state)
    }

    pub fn watermark_callback(
        t: &Timestamp,
        watermark_state: &mut TimeVersionedState<usize, ()>,
        cb_state: &TimeVersionedState<(), usize>,
        write_stream: &mut WriteStream<Option<usize>>,
    ) {
        let data = Self::watermark_callback_helper(t, watermark_state, cb_state);
        let msg = Message::new_message(t.clone(), data);
        write_stream.send(msg).unwrap();
    }
}

impl Operator for TimeVersionedStateOp {}

/// Sends messages to the TimeVersionedStateOp, which deterministically and
/// recursively sends s(t) = sum(messages(t)) - s(t - 1).
#[test]
fn test_time_versioned_state() {
    let config = utils::make_default_config();
    let node = Node::new(config);

    let mut ingest_stream = IngestStream::new(0);
    let sum_stream = connect_1_write!(
        TimeVersionedStateOp,
        OperatorConfig::new()
            .name("TimeVersionedStateOp")
            .flow_watermarks(false),
        ingest_stream
    );

    let mut extract_stream = ExtractStream::new(0, &sum_stream);

    node.run_async();

    let mut previous_state = 0;
    for i in 0..5 {
        let mut state = 0;
        // Send messages with data i, i + 1, i + 2.
        let current_time = Timestamp::new(vec![i as u64]);
        for j in i..i + 3 {
            let msg = Message::new_message(current_time.clone(), j);
            state += j;
            ingest_stream.send(msg).unwrap();
        }
        state -= previous_state;
        // Send watermark to close time for i.
        let watermark = Message::new_watermark(current_time.clone());
        ingest_stream.send(watermark).unwrap();

        // Should receive message equal to state.
        let msg = extract_stream.read().unwrap();
        let expected_msg = Message::new_message(current_time, Some(state));
        assert_eq!(msg, expected_msg);

        previous_state = state;
    }
}
