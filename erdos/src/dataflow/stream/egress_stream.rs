use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use serde::Deserialize;

use crate::dataflow::{Data, Message};

use super::{
    errors::{ReadError, TryReadError},
    OperatorStream, ReadStream, Stream, StreamId,
};

/// An [`EgressStream`] enables drivers to read data from a running ERDOS application.
///
/// Similar to a [`ReadStream`], an [`EgressStream`] exposes [`read`](EgressStream::read) and
/// [`try_read`](EgressStream::try_read) functions to allow drivers to read data output by the
/// operators of the graph.
///
/// # Example
/// The below example shows how to use an [`IngressStream`](crate::dataflow::stream::IngressStream)
/// to send data to a [`FlatMapOperator`](crate::dataflow::operators::FlatMapOperator),
/// and retrieve the processed values through an [`EgressStream`].
/// ```no_run
/// # use erdos::dataflow::{
/// #    stream::{IngestStream, EgressStream, Stream},
/// #    operators::FlatMapOperator,
/// #    OperatorConfig, Message, Timestamp
/// # };
/// # use erdos::*;
/// # use erdos::node::Node;
/// #
/// let args = erdos::new_app("ERDOS").get_matches();
/// let mut node = Node::new(Configuration::from_args(&args));
///
/// // Create an IngestStream.
/// let mut ingest_stream = IngestStream::new();
///
/// // Create an ExtractStream from the ReadStream of the FlatMapOperator.
/// let output_stream = erdos::connect_one_in_one_out(
///     || FlatMapOperator::new(|x: &usize| { std::iter::once(2 * x) }),
///     || {},
///     OperatorConfig::new().name("MapOperator"),
///     &ingest_stream,
/// );
/// let mut extract_stream = ExtractStream::new(&output_stream);
///
/// node.run_async();
///
/// // Send data on the IngestStream.
/// for i in 1..10 {
///     ingest_stream.send(Message::new_message(Timestamp::Time(vec![i as u64]), i)).unwrap();
/// }
///
/// // Retrieve mapped values using an ExtractStream.
/// for i in 1..10 {
///     let message = extract_stream.read().unwrap();
///     assert_eq!(*message.data().unwrap(), 2 * i);
/// }
/// ```
pub struct EgressStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// The unique ID of the stream (automatically generated by the constructor)
    id: StreamId,
    /// The name of the stream (copied from the input stream)
    name: String,
    /// The ReadStream associated with the EgressStream.
    read_stream_option: Arc<Mutex<Option<ReadStream<D>>>>,
}

impl<D> EgressStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// Returns a new instance of the [`EgressStream`].
    ///
    /// # Arguments
    /// * `stream`: The [`Stream`] returned by an [operator](crate::dataflow::operator)
    /// from which to extract messages.
    pub fn new(stream: &OperatorStream<D>) -> Self {
        tracing::debug!(
            "Initializing an EgressStream with the ReadStream {} (ID: {})",
            stream.name(),
            stream.id(),
        );

        Self {
            id: stream.id(),
            name: stream.name(),
            read_stream_option: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn get_read_stream(&self) -> Arc<Mutex<Option<ReadStream<D>>>> {
        Arc::clone(&self.read_stream_option)
    }

    /// Returns `true` if a top watermark message was sent or the [`EgressStream`] failed to set
    /// up.
    pub fn is_closed(&self) -> bool {
        self.read_stream_option
            .lock()
            .unwrap()
            .as_ref()
            .map(ReadStream::is_closed)
            .unwrap_or(true)
    }

    /// Non-blocking read from the [`EgressStream`].
    ///
    /// Returns the Message available on the [`ReadStream`], or an [`Empty`](TryReadError::Empty)
    /// if no message is available.
    pub fn try_read(&mut self) -> Result<Message<D>, TryReadError> {
        match self.read_stream_option.lock().unwrap().as_mut() {
            Some(read_stream) => read_stream.try_read(),
            None => Err(TryReadError::Disconnected),
        }
    }

    /// Blocking read from the [`EgressStream`].
    ///
    /// Returns the Message available on the [`ReadStream`].
    pub fn read(&mut self) -> Result<Message<D>, ReadError> {
        loop {
            match self.try_read() {
                Ok(msg) => return Ok(msg),
                Err(TryReadError::Disconnected) => return Err(ReadError::Disconnected),
                Err(TryReadError::Empty) => (),
                Err(TryReadError::SerializationError) => return Err(ReadError::SerializationError),
                Err(TryReadError::Closed) => return Err(ReadError::Closed),
            };
            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn id(&self) -> StreamId {
        self.id
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}

// Needed to avoid deadlock in Python
unsafe impl<D> Send for EgressStream<D> where for<'a> D: Data + Deserialize<'a> {}
