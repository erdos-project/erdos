use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use serde::Deserialize;

use crate::dataflow::{
    message::Message, stream::WriteStreamT, Data, Operator, OperatorConfig, ReadStream, Timestamp,
    WriteStream,
};

/// A structure that stores the state associated with a stream for the JoinOperator, and provides
/// the associated functions for mutation of the data.
/// Uses a ConcurrentHashMap to store the messages and a min-heap to ensure easy retrieval of the
/// timestamps for cleaning.
#[derive(Clone)]
struct StreamState<D: Data> {
    msgs: Arc<RwLock<HashMap<Timestamp, Vec<D>>>>,
    // A min-heap tracking the keys of the hashmap.
    timestamps: Arc<RwLock<BinaryHeap<Reverse<Timestamp>>>>,
}

impl<D: Data> StreamState<D> {
    fn new() -> Self {
        Self {
            msgs: Arc::new(RwLock::new(HashMap::new())),
            timestamps: Arc::new(RwLock::new(BinaryHeap::new())),
        }
    }

    /// Adds a message to the ConcurrentHashMap.
    fn add_msg(&mut self, timestamp: &Timestamp, msg: D) {
        // Insert a new vector if the key does not exist, and add the key to the timestamps.
        let mut msgs = self.msgs.write().unwrap();
        match msgs.get_mut(timestamp) {
            Some(msg_vec) => msg_vec.push(msg),
            None => {
                msgs.insert(timestamp.clone(), vec![msg]);
                self.timestamps
                    .write()
                    .unwrap()
                    .push(Reverse(timestamp.clone()));
            }
        };
    }

    /// Cleans the state corresponding to a given Timestamp (upto and including).
    fn clean_state(&self, timestamp: &Timestamp) {
        let timestamps = &mut self.timestamps.write().unwrap();
        while timestamps.peek().map_or(false, |t| t.0 <= *timestamp) {
            let t = timestamps.pop().unwrap().0;
            self.msgs
                .write()
                .unwrap()
                .remove(&t)
                .expect("StreamState: expected Timestamp to be present");
        }
    }

    /// Retrieve the state.
    fn get_state(&self, timestamp: &Timestamp) -> Option<Vec<D>> {
        match self.msgs.read().unwrap().get(timestamp) {
            Some(value) => Some(value.clone()),
            None => None,
        }
        //(*((*(self.msgs.read().unwrap())).get(timestamp).unwrap())).clone()
    }
}

/// An operator that joins two incoming streams of type D1 and D2 into a stream of type D3 using
/// the function provided.
///
/// # Example
/// The below example shows how to use a JoinOperator to sum two streams of incoming u32 messages,
/// and return them as u64 messages.
///
/// ```
/// # use erdos::dataflow::{stream::IngestStream, operators::JoinOperator, OperatorConfig};
/// # use erdos::*;
/// #
/// # let mut left_u32_stream = IngestStream::new(0);
/// # let mut right_u32_stream = IngestStream::new(0);
/// #
/// // Add the joining function as an argument to the operator via the OperatorConfig.
/// let join_config = OperatorConfig::new()
///     .name("JoinOperator")
///     .arg(|left_data: Vec<u32>, right_data: Vec<u32>| -> u64 {
///         (left_data.iter().sum::<u32>() + right_data.iter().sum::<u32>()) as u64
///     });
/// let output_stream = connect_1_write!(
///     JoinOperator<u32, u32, u64>, join_config,left_u32_stream, right_u32_stream);
/// ```
pub struct JoinOperator<D1: Data, D2: Data, D3: Data> {
    phantom_data: PhantomData<(D1, D2, D3)>,
}

impl<'a, D1: Data, D2: Data, D3: Data + Deserialize<'a>> JoinOperator<D1, D2, D3> {
    /// Returns a new instance of the JoinOperator.
    ///
    /// # Arguments
    /// * `config` - An instance of OperatorConfig that provides the closure used to join items of
    /// type Vec<D1> and Vec<D2> to a value of type D3.
    /// * `input_stream_left` - Represents the incoming stream of messages of type D1.
    /// * `input_stream_right` - Represents the incoming stream of messages of type D2.
    /// * `output_stream` - Represents an outgoing stream of messages of type D3.
    pub fn new<F: 'static + Clone + Fn(Vec<D1>, Vec<D2>) -> D3>(
        config: OperatorConfig<F>,
        input_stream_left: ReadStream<D1>,
        input_stream_right: ReadStream<D2>,
        output_stream: WriteStream<D3>,
    ) -> Self {
        let name = match config.name {
            Some(s) => s,
            None => format!("JoinOperator {}", config.id),
        };

        // Package the state with the left stream and add a callback to the new stream.
        let stateful_stream_left = input_stream_left.add_state(StreamState::<D1>::new());
        stateful_stream_left.add_callback(Self::on_left_data_callback);

        // Package the state with the right stream and add a callback to the new stream.
        let stateful_stream_right = input_stream_right.add_state(StreamState::<D2>::new());
        stateful_stream_right.add_callback(Self::on_right_data_callback);

        let cb = config
            .arg
            .unwrap_or_else(|| panic!("{}: no join function provided", name));
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
            phantom_data: PhantomData,
        }
    }

    /// The function to be called when a message is received on the left input stream.
    /// This callback adds the data received in the message to the state associated with the
    /// stream.
    fn on_left_data_callback(t: &Timestamp, msg: &D1, state: &mut StreamState<D1>) {
        state.add_msg(t, msg.clone());
    }

    /// The function to be called when a message is received on the right input stream.
    /// This callback adds the data received in the message to the state associated with the
    /// stream.
    fn on_right_data_callback(t: &Timestamp, msg: &D2, state: &mut StreamState<D2>) {
        state.add_msg(t, msg.clone());
    }

    /// The function to be called when a watermark is received on both the left and the right
    /// streams.
    /// This callback uses the saved state from the two streams and joins them using the provided
    /// closure.
    fn on_watermark_callback<F: 'static + Clone + Fn(Vec<D1>, Vec<D2>) -> D3>(
        t: &Timestamp,
        left_state: &StreamState<D1>,
        right_state: &StreamState<D2>,
        write_stream: &mut WriteStream<D3>,
        join_function: &F,
    ) {
        // Retrieve the state and run the given callback on it.
        let left_state_t: Vec<D1> = left_state.get_state(t).unwrap();
        let right_state_t: Vec<D2> = right_state.get_state(t).unwrap();
        let result_t: D3 = join_function(left_state_t, right_state_t);

        // Send the result on the write stream.
        write_stream
            .send(Message::new_message(t.clone(), result_t))
            .expect("JoinOperator: error sending on write stream");

        // Garbage collect all the data upto and including this timestamp.
        left_state.clean_state(t);
        right_state.clean_state(t);
    }

    pub fn connect(
        _left_read_stream: &ReadStream<D1>,
        _right_read_stream: &ReadStream<D2>,
    ) -> WriteStream<D3> {
        WriteStream::new()
    }
}

impl<'a, D1: Data, D2: Data, D3: Data + Deserialize<'a>> Operator for JoinOperator<D1, D2, D3> {}
