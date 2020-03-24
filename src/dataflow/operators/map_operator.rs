use crate::dataflow::message::Message;
use crate::dataflow::{
    stream::WriteStreamT, Data, Operator, OperatorConfig, ReadStream, Timestamp, WriteStream,
};
use serde::Deserialize;
use std::marker::PhantomData;

/// An operator that maps an incoming stream of type D1 to a stream of type D2 using the provided
/// closure.
///
/// # Example
/// The below example shows how to use a MapOperator to double an incoming stream of u32 messages,
/// and return them as u64 messages.
///
/// ```
/// use erdos::dataflow::{stream::IngestStream, operators::MapOperator, OperatorConfig};
/// use erdos::*;
/// let mut map_config = OperatorConfig::new();
/// map_config.name("MapOperator").arg(
///     |data: u32| -> u64 { (data * 2) as u64 });
/// let mut ingest_stream = IngestStream::new(0);
/// let output_read_stream = connect_1_write!(MapOperator<u32, u64>, map_config, ingest_stream);
/// ```
pub struct MapOperator<D1: Data, D2: Data> {
    /// The name given to the specific instance of the MapOperator.
    name: String,
    phantom_data: PhantomData<(D1, D2)>,
}

impl<'a, D1: Data, D2: Data + Deserialize<'a>> MapOperator<D1, D2> {
    /// Returns a new instance of the MapOperator.
    ///
    /// # Arguments
    /// * `config` - An instance of OperatorConfig that provides the closure used to map items of
    /// type D1 to D2.
    /// * `input_stream` - Represents the incoming stream of messages of type D1.
    /// * `output_stream` - Represents an outgoing stream of messages of type D2.
    pub fn new<F: 'static + Clone + Fn(D1) -> D2>(
        config: OperatorConfig<F>,
        input_stream: ReadStream<D1>,
        output_stream: WriteStream<D2>,
    ) -> Self {
        // TODO :: We do this because otherwise we would either have to clone the output stream or
        // mutex the original output stream. This code should be fixed once add_callback passes
        // output streams by default.
        let stateful_stream = input_stream.add_state(output_stream);

        // Clone the name so that we can move the passed function into the callback.
        let name: String = config
            .name
            .clone()
            .unwrap_or_else(|| format!("MapOperator {}", config.id));
        let callback = config
            .arg
            .unwrap_or_else(|| panic!("{}: no map function supplied", name));

        stateful_stream.add_callback(
            move |t: Timestamp, msg: D1, output_stream: &mut WriteStream<_>| {
                Self::on_data_callback(t, msg, output_stream, &callback)
            },
        );
        Self {
            name,
            phantom_data: PhantomData,
        }
    }

    /// Returns a new instance of a WriteStream to send its outgoing messages on.
    ///
    /// # Arguments
    /// * `input_stream` - Represents the incoming stream of messages of type D1.
    pub fn connect(_input_stream: &ReadStream<D1>) -> WriteStream<D2> {
        WriteStream::new()
    }

    /// The callback function to be invoked upon receipt of a message on the input stream.
    ///
    /// # Arguments
    /// * `t` - The timestamp of the message.
    /// * `msg` - The incoming message on the input stream.
    /// * `output_stream` - A handle to the output stream to write the output to.
    /// * `map_function` - A reference to the function to invoke for the message.
    fn on_data_callback<F: 'static + Clone + Fn(D1) -> D2>(
        t: Timestamp,
        msg: D1,
        output_stream: &mut WriteStream<D2>,
        map_function: &F,
    ) {
        let result: D2 = map_function(msg);
        output_stream.send(Message::new_message(t, result));
    }
}

impl<'a, D1: Data, D2: Data + Deserialize<'a>> Operator for MapOperator<D1, D2> {}
