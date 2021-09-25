use std::{
    borrow::BorrowMut,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use serde::Deserialize;

use crate::{
    dataflow::{
        graph::{default_graph, StreamSetupHook},
        Data, Message,
    },
    scheduler::channel_manager::ChannelManager,
};

use super::{
    errors::{ReadError, TryReadError},
    ReadStream, Stream, StreamId,
};

/// An [`ExtractStream`] enables drivers to read data from a running ERDOS application.
///
/// Similar to a [`ReadStream`], an [`ExtractStream`] exposes [`read`](ExtractStream::read) and
/// [`try_read`](ExtractStream::try_read) functions to allow drivers to read data output by the
/// operators of the graph.
///
/// # Example
/// The below example shows how to use an [`IngestStream`] to send data to a
/// [`MapOperator`](crate::dataflow::operators::MapOperator), and retrieve the mapped values
/// through an [`ExtractStream`].
/// ```
/// # use erdos::dataflow::{
/// #    stream::{IngestStream, ExtractStream},
/// #    operators::MapOperator,
/// #    OperatorConfig, Message, Timestamp
/// # };
/// # use erdos::*;
/// #
/// # let map_config = OperatorConfig::new()
//  #     .name("MapOperator")
/// #     .arg(|data: &u32| -> u64 { (data * 2) as u64 });
/// #
/// // Create an IngestStream.
/// let mut ingest_stream = IngestStream::new(); // or IngestStream::new_with_name("driver")
///
/// // Create an ExtractStream from the ReadStream of the MapOperator.
/// let output_read_stream = connect_1_write!(MapOperator<u32, u64>, map_config, ingest_stream);
/// let mut extract_stream = ExtractStream::new(0, &output_read_stream);
///
/// // Send data on the IngestStream.
/// for i in 1..10 {
///     // We expect an error because we have not started the dataflow graph yet.
///     match ingest_stream.send(Message::new_message(Timestamp::new(vec![i as u64]), i)) {
///         Err(e) => (),
///         _ => (),
///     };
/// }
///
/// // Retrieve mapped values using an ExtractStream.
/// for i in 1..10 {
///     // We expect a Disconnected error because we have not started the dataflow graph yet.
///     match extract_stream.try_read() {
///         Err(e) => (),
///         _ => (),
///     };
/// }
/// ```
pub struct ExtractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// The unique ID of the stream (automatically generated by the constructor)
    id: StreamId,
    /// The ReadStream associated with the ExtractStream.
    read_stream_option: Option<ReadStream<D>>,
    // Used to circumvent requiring Send to transfer ReadStream across threads
    channel_manager_option: Arc<Mutex<Option<Arc<Mutex<ChannelManager>>>>>,
}

impl<D> ExtractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// Returns a new instance of the [`ExtractStream`]
    ///
    /// # Arguments
    /// * `read_stream`: The [`ReadStream`] returned by an
    /// [`Operator`](crate::dataflow::operator::Operator) to extract the messages from.
    pub fn new(read_stream: &Stream<D>) -> Self {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Initializing an ExtractStream with the ReadStream {} (ID: {})",
            read_stream.name(),
            read_stream.id(),
        );

        let id = read_stream.id();

        // Create the ExtractStream structure.
        let extract_stream = Self {
            id,
            read_stream_option: None,
            channel_manager_option: Arc::new(Mutex::new(None)),
        };

        default_graph::add_extract_stream(&extract_stream);
        extract_stream
    }

    /// Returns `true` if a top watermark message was sent or the [`ExtractStream`] failed to set
    /// up.
    pub fn is_closed(&self) -> bool {
        self.read_stream_option
            .as_ref()
            .map(ReadStream::is_closed)
            .unwrap_or(true)
    }

    /// Non-blocking read from the [`ExtractStream`].
    ///
    /// Returns the Message available on the [`ReadStream`], or an [`Empty`](TryReadError::Empty)
    /// if no message is available.
    pub fn try_read(&mut self) -> Result<Message<D>, TryReadError> {
        if let Some(read_stream) = self.read_stream_option.borrow_mut() {
            read_stream.try_read()
        } else {
            // Try to setup read stream
            if let Some(channel_manager) = &*self.channel_manager_option.lock().unwrap() {
                match channel_manager.lock().unwrap().take_recv_endpoint(self.id) {
                    Ok(recv_endpoint) => {
                        let mut read_stream = ReadStream::new(
                            self.id,
                            &default_graph::get_stream_name(&self.id),
                            Some(recv_endpoint),
                        );

                        let result = read_stream.try_read();
                        self.read_stream_option.replace(read_stream);
                        return result;
                    }
                    Err(msg) => slog::error!(
                        crate::TERMINAL_LOGGER,
                        "ExtractStream {} (ID: {}): error getting endpoint from \
                        channel manager \"{}\"",
                        self.name(),
                        self.id(),
                        msg
                    ),
                }
            }
            Err(TryReadError::Disconnected)
        }
    }

    /// Blocking read from the [`ExtractStream`].
    ///
    /// Returns the Message available on the [`ReadStream`].
    pub fn read(&mut self) -> Result<Message<D>, ReadError> {
        loop {
            let result = self.try_read();
            if self.read_stream_option.is_some() {
                match result {
                    Ok(msg) => return Ok(msg),
                    Err(TryReadError::Disconnected) => return Err(ReadError::Disconnected),
                    Err(TryReadError::Empty) => (),
                    Err(TryReadError::SerializationError) => {
                        return Err(ReadError::SerializationError)
                    }
                    Err(TryReadError::Closed) => return Err(ReadError::Closed),
                };
            } else {
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    pub fn id(&self) -> StreamId {
        self.id
    }

    pub fn name(&self) -> String {
        default_graph::get_stream_name(&self.id)
    }

    /// Returns a function that sets up self.read_stream_option using the channel_manager.
    pub(crate) fn get_setup_hook(&self) -> impl StreamSetupHook {
        let channel_manager_option_copy = Arc::clone(&self.channel_manager_option);

        move |channel_manager: Arc<Mutex<ChannelManager>>| {
            channel_manager_option_copy
                .lock()
                .unwrap()
                .replace(channel_manager);
        }
    }
}

// Needed to avoid deadlock in Python
unsafe impl<D> Send for ExtractStream<D> where for<'a> D: Data + Deserialize<'a> {}
