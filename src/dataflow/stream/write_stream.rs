use std::{
    fmt,
    sync::{Arc, Mutex},
};

use serde::Deserialize;

use crate::{
    communication::{Pusher, SendEndpoint},
    dataflow::{
        deadlines::{CondFn, ConditionContext},
        Data, Message, Timestamp,
    },
};

use super::{errors::WriteStreamError, StreamId, StreamT, WriteStreamT};

// TODO (Sukrit) :: This example needs to be fixed after we enable attaching WriteStreams to
// callbacks for normal read streams.
/// A [`WriteStream`] allows operators to send data to other operators.
///
/// An [`Operator`](crate::dataflow::operator::Operator) creates and returns [`WriteStream`] s in
/// its `connect` function. Operators receive the returned [`WriteStream`] s in their `new`
/// function, which can be attached to registered callbacks in order to send messages on the
/// streams.
///
/// A driver receives a set of [`ReadStream`] s corresponding to the [`WriteStream`] s returned by
/// an operator's `connect` function. In order to send data from the driver to an operator, the
/// driver can instantiate an [`IngestStream`] and pass it to the operator's [`connect`] function.
///
/// # Example
/// The following example shows an [`Operator`](crate::dataflow::operator::Operator) that takes a
/// single [`ReadStream`] and maps the received value to its square and send that on a
/// [`WriteStream`]:
/// ```
/// use erdos::dataflow::message::Message;
/// use erdos::dataflow::{
///     stream::WriteStreamT, Operator, ReadStream, Timestamp, WriteStream, OperatorConfig
/// };
/// pub struct SquareOperator {}
///
/// impl SquareOperator {
///     pub fn new(
///         config: OperatorConfig<()>,
///         input_stream: ReadStream<u32>,
///         write_stream: WriteStream<u64>,
///     ) -> Self {
///         let stateful_read_stream = input_stream.add_state(write_stream);
///         // Request a callback upon receipt of every message.
///         stateful_read_stream.add_callback(
///             move |t: &Timestamp, msg: &u32, stream: &mut WriteStream<u64>| {
///                 Self::on_callback(t, msg, stream);
///             },
///         );
///         Self {}
///     }
///
///     pub fn connect(input_stream: ReadStream<u32>) -> WriteStream<u64> {
///         WriteStream::new()
///     }
///
///     fn on_callback(t: &Timestamp, msg: &u32, write_stream: &mut WriteStream<u64>) {
///         write_stream.send(Message::new_message(t.clone(), (msg * msg) as u64));
///     }
/// }
///
/// impl Operator for SquareOperator {}
/// ```

#[derive(Clone)]
pub struct WriteStream<D: Data> {
    /// The unique ID of the stream (automatically generated by the constructor)
    id: StreamId,
    /// The name of the stream (String representation of the ID, if no name provided)
    name: String,
    /// Sends message to other operators.
    pusher: Option<Pusher<Arc<Message<D>>>>,
    /// Statistics about this instance of the write stream.
    stats: Arc<Mutex<WriteStreamStatistics>>,
}

impl<D: Data> WriteStream<D> {
    /// Creates the [`WriteStream`] to be used to send messages to the dataflow.
    pub(crate) fn new(id: StreamId, name: &str) -> Self {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Initializing a WriteStream {} with the ID: {}",
            name,
            id
        );
        Self {
            id,
            name: name.to_string(),
            pusher: Some(Pusher::new()),
            stats: Arc::new(Mutex::new(WriteStreamStatistics::new())),
        }
    }

    pub fn from_endpoints(endpoints: Vec<SendEndpoint<Arc<Message<D>>>>, id: StreamId) -> Self {
        let mut stream = Self::new(id, &id.to_string());
        for endpoint in endpoints {
            stream.add_endpoint(endpoint);
        }
        stream
    }

    /// Returns `true` if a top watermark message was received or the [`IngestStream`] failed to
    /// set up.
    pub fn is_closed(&self) -> bool {
        self.stats.lock().unwrap().is_stream_closed()
    }

    fn add_endpoint(&mut self, endpoint: SendEndpoint<Arc<Message<D>>>) {
        self.pusher
            .as_mut()
            .expect("Attempted to add endpoint to WriteStream, however no pusher exists")
            .add_endpoint(endpoint);
    }

    /// Closes the stream for future messages.
    fn close_stream(&mut self) {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Closing write stream {} (ID: {})",
            self.name(),
            self.id()
        );
        self.stats.lock().unwrap().close_stream();
        self.pusher = None;
    }

    /// Updates the last watermark received on the stream.
    ///
    /// # Arguments
    /// * `msg` - The message to be sent on the stream.
    fn update_statistics(&mut self, msg: &Message<D>) -> Result<(), WriteStreamError> {
        match msg {
            Message::TimestampedData(td) => {
                let mut stats = self.stats.lock().unwrap();
                if td.timestamp < *stats.low_watermark() {
                    return Err(WriteStreamError::TimestampError);
                }
                // Increment the message count.
                stats
                    .condition_context
                    .increment_msg_count(self.id(), td.timestamp.clone())
            }
            Message::Watermark(msg_watermark) => {
                let mut stats = self.stats.lock().unwrap();
                if msg_watermark < stats.low_watermark() {
                    return Err(WriteStreamError::TimestampError);
                }
                slog::debug!(
                    crate::TERMINAL_LOGGER,
                    "Updating watermark on WriteStream {} (ID: {}) from {:?} to {:?}",
                    self.name(),
                    self.id(),
                    stats.low_watermark(),
                    msg_watermark
                );
                stats.update_low_watermark(msg_watermark.clone());

                // Notify the arrival of the watermark.
                stats
                    .condition_context
                    .notify_watermark_arrival(self.id(), msg_watermark.clone());
            }
        }
        Ok(())
    }

    /// Evaluates a condition function on the statistics of the write stream.
    pub fn evaluate_condition(&self, condition_fn: &CondFn) -> bool {
        (condition_fn)(&self.stats.lock().unwrap().condition_context)
    }

    pub(crate) fn get_statistics(&self) -> Arc<Mutex<WriteStreamStatistics>> {
        Arc::clone(&self.stats)
    }

    /// Clears the condition context state.
    pub fn clear_state(&mut self, timestamp: Timestamp) {
        self.stats
            .lock()
            .unwrap()
            .condition_context
            .clear_state(self.id(), timestamp);
    }
}

impl<D: Data> fmt::Debug for WriteStream<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WriteStream {{ id: {}, low_watermark: {:?} }}",
            self.id,
            self.stats.lock().unwrap().low_watermark,
        )
    }
}

impl<D: Data> StreamT<D> for WriteStream<D> {
    /// Get the ID given to the stream by the constructor
    fn id(&self) -> StreamId {
        self.id
    }

    /// Get the name of the stream.
    /// Returns a [`str`] version of the ID if the stream was not constructed with
    /// [`new_with_name`](WriteStream::new_with_name).
    fn name(&self) -> &str {
        &self.name[..]
    }
}

impl<'a, D: Data + Deserialize<'a>> WriteStreamT<D> for WriteStream<D> {
    fn send(&mut self, msg: Message<D>) -> Result<(), WriteStreamError> {
        // Check if the stream was closed before, and return an error.
        if self.is_closed() {
            slog::warn!(
                crate::TERMINAL_LOGGER,
                "Trying to send messages on a closed WriteStream {} (ID: {})",
                self.name(),
                self.id(),
            );
            return Err(WriteStreamError::Closed);
        }

        // Close the stream later if the message being sent represents the top watermark.
        let mut close_stream: bool = false;
        if msg.is_top_watermark() {
            slog::debug!(
                crate::TERMINAL_LOGGER,
                "Sending top watermark on the stream {} (ID: {}).",
                self.name(),
                self.id()
            );
            close_stream = true;
        }

        // Update the watermark and send the message forward.
        self.update_statistics(&msg)?;
        let msg_arc = Arc::new(msg);

        match self.pusher.as_mut() {
            Some(pusher) => pusher.send(msg_arc).map_err(WriteStreamError::from)?,
            None => {
                slog::debug!(
                    crate::TERMINAL_LOGGER,
                    "No Pusher was found for the WriteStream {} (ID: {}). \
                             Skipping message sending.",
                    self.name(),
                    self.id()
                );
                ()
            }
        };

        // If we received a top watermark, close the stream.
        if close_stream {
            self.close_stream();
        }
        Ok(())
    }
}

/// Maintains statistics on the WriteStream required for the maintenance of the watermarks, and the
/// execution of end conditions for deadlines.
pub(crate) struct WriteStreamStatistics {
    low_watermark: Timestamp,
    is_stream_closed: bool,
    condition_context: ConditionContext,
}

impl WriteStreamStatistics {
    fn new() -> Self {
        Self {
            low_watermark: Timestamp::Bottom,
            is_stream_closed: false,
            condition_context: ConditionContext::new(),
        }
    }

    /// Closes the stream.
    fn close_stream(&mut self) {
        self.low_watermark = Timestamp::Top;
        self.is_stream_closed = true;
    }

    /// Is the stream closed?
    fn is_stream_closed(&self) -> bool {
        self.is_stream_closed
    }

    /// Returns the current low watermark on the stream.
    fn low_watermark(&self) -> &Timestamp {
        &self.low_watermark
    }

    /// Update the low watermark of the corresponding stream.
    /// Increases the low watermark only if watermark_timestamp > current low watermark.
    fn update_low_watermark(&mut self, watermark_timestamp: Timestamp) {
        if self.low_watermark < watermark_timestamp {
            self.low_watermark = watermark_timestamp;
        }
    }

    /// Get the ConditionContext saved in the stream.
    pub(crate) fn get_condition_context(&self) -> &ConditionContext {
        &self.condition_context
    }
}
