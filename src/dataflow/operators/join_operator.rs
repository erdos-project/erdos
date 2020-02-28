use crate::dataflow::message::Message;
use crate::dataflow::stream::WriteStreamT;
use crate::dataflow::{Data, OperatorConfig, ReadStream, Timestamp, WriteStream};
use chashmap::CHashMap;
use serde::Deserialize;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct StreamState<D: Data> {
    // We use a CHashMap because multiple callbacks can be simultaneously invoked.
    msgs: CHashMap<Timestamp, Vec<D>>,
    // A min-heap tracking the keys of the hashmap.
    _timestamps: BinaryHeap<Reverse<Timestamp>>,
}

impl<D: Data> StreamState<D> {
    pub fn new() -> Self {
        Self {
            msgs: CHashMap::new(),
            _timestamps: BinaryHeap::new(),
        }
    }

    pub fn add_msg(&mut self, timestamp: &Timestamp, msg: D) {
        // TODO:: Replace with upsert.
        // Insert a new Vector if the key does not exist, and add the key to the timestamps.
        if !self.msgs.contains_key(timestamp) {
            self.msgs.insert_new(timestamp.clone(), Vec::new());
            self._timestamps.push(Reverse(timestamp.clone()));
        }

        // Get the vector corresponding to the given key and acquire a write lock on it.
        let mut messages = self.msgs.get_mut(timestamp).unwrap();
        messages.push(msg);
        // The data structure automatically releases the write lock once the guard goes out of
        // scope.
    }

    pub fn clean_state(&mut self, timestamp: &Timestamp) {
        while self._timestamps.peek().unwrap().0 < *timestamp {
            let _t = self._timestamps.pop().unwrap().0;
            self.msgs.remove(&_t).unwrap();
        }
    }

    pub fn get_state(&self, timestamp: &Timestamp) -> Vec<D> {
        (*(self.msgs.get(timestamp).unwrap())).clone()
    }
}

pub struct JoinOperator<D1: Data, D2: Data, D3: Data> {
    name: String,
    phantom_data: PhantomData<(D1, D2, D3)>,
}

impl<'a, D1: Data, D2: Data, D3: Data + Deserialize<'a>> JoinOperator<D1, D2, D3> {
    pub fn new<F: 'static + Clone + Fn(Vec<D1>, Vec<D2>) -> D3>(
        config: OperatorConfig<F>,
        input_stream_left: ReadStream<D1>,
        input_stream_right: ReadStream<D2>,
        output_stream: WriteStream<D3>,
    ) -> Self {
        // Package the state with the left stream and add a callback to the new stream.
        let stateful_stream_left = input_stream_left.add_state(StreamState::<D1>::new());
        stateful_stream_left.add_callback(Self::on_left_data_callback);
        stateful_stream_left.add_watermark_callback(Self::on_left_watermark_data_callback);

        // Package the state with the right stream and add a callback to the new stream.
        let stateful_stream_right = input_stream_right.add_state(StreamState::<D2>::new());
        stateful_stream_right.add_callback(Self::on_right_data_callback);
        stateful_stream_right.add_watermark_callback(Self::on_right_watermark_data_callback);

        let cb = config.arg;
        stateful_stream_left
            .add_read_stream(&stateful_stream_right)
            .borrow_mut()
            .add_write_stream(&output_stream)
            .borrow_mut()
            .add_watermark_callback(
                move |t: &Timestamp,
                      left_state: &StreamState<D1>,
                      right_state: &StreamState<D2>,
                      write_stream: &mut WriteStream<D3>| {
                    Self::on_watermark_callback(t, left_state, right_state, write_stream, &cb)
                },
            );

        Self {
            name: config.name,
            phantom_data: PhantomData,
        }
    }

    fn on_left_data_callback(t: Timestamp, msg: D1, state: &mut StreamState<D1>) {
        state.add_msg(&t, msg);
    }

    fn on_left_watermark_data_callback<'r, 's>(t: &'r Timestamp, state: &'s mut StreamState<D1>) {
        state.clean_state(t);
    }

    fn on_right_data_callback(t: Timestamp, msg: D2, state: &mut StreamState<D2>) {
        state.add_msg(&t, msg);
    }

    fn on_right_watermark_data_callback<'r, 's>(t: &'r Timestamp, state: &'s mut StreamState<D2>) {
        state.clean_state(&t);
    }

    fn on_watermark_callback<F: 'static + Clone + Fn(Vec<D1>, Vec<D2>) -> D3>(
        t: &Timestamp,
        left_state: &StreamState<D1>,
        right_state: &StreamState<D2>,
        write_stream: &mut WriteStream<D3>,
        join_function: &F,
    ) {
        // Retrieve the state and run the given callback on it.
        let left_state_t: Vec<D1> = left_state.get_state(t);
        let right_state_t: Vec<D2> = right_state.get_state(t);
        let result_t: D3 = join_function(left_state_t, right_state_t);

        // Send the result on the write stream.
        write_stream.send(Message::new_message(t.clone(), result_t));
    }

    pub fn connect(
        input_stream_left: &ReadStream<D1>,
        input_stream_right: &ReadStream<D2>,
    ) -> WriteStream<D3> {
        WriteStream::new()
    }

    pub fn run(&self) {}
}
