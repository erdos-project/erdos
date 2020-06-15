use std::{fmt, sync::Arc};

use serde::Deserialize;

use crate::{
    communication::{Pusher, SendEndpoint},
    dataflow::{Data, Message, Timestamp, WriteStreamError},
};

use super::{StreamId, WriteStreamT};

// TODO: refactor with internal write stream
#[derive(Clone)]
pub struct WriteStream<D: Data> {
    /// StreamId of the stream.
    id: StreamId,
    /// User-defined stream name.
    name: String,
    /// Sends message to other operators.
    pusher: Option<Pusher<Arc<Message<D>>>>,
    /// Current low watermark.
    low_watermark: Timestamp,
    /// Whether the stream is closed.
    stream_closed: bool,
}

impl<D: Data> WriteStream<D> {
    pub fn new() -> Self {
        let id = StreamId::new_deterministic();
        WriteStream::new_internal(id, id.to_string())
    }

    pub fn new_from_name(name: &str) -> Self {
        WriteStream::new_internal(StreamId::new_deterministic(), name.to_string())
    }

    pub fn new_from_id(id: StreamId) -> Self {
        WriteStream::new_internal(id, id.to_string())
    }

    fn new_internal(id: StreamId, name: String) -> Self {
        Self {
            id,
            name,
            pusher: Some(Pusher::new()),
            low_watermark: Timestamp::new(vec![0]),
            stream_closed: false,
        }
    }

    pub fn from_endpoints(endpoints: Vec<SendEndpoint<Arc<Message<D>>>>, id: StreamId) -> Self {
        let mut stream = Self::new_from_id(id);
        for endpoint in endpoints {
            stream.add_endpoint(endpoint);
        }
        stream
    }

    pub fn get_id(&self) -> StreamId {
        self.id
    }

    pub fn get_name(&self) -> &str {
        &self.name[..]
    }

    pub fn is_closed(&self) -> bool {
        self.stream_closed
    }

    fn add_endpoint(&mut self, endpoint: SendEndpoint<Arc<Message<D>>>) {
        self.pusher
            .as_mut()
            .expect("Attempted to add endpoint to WriteStream, however no pusher exists")
            .add_endpoint(endpoint);
    }

    fn close_stream(&mut self) {
        let logger = crate::get_terminal_logger();
        slog::debug!(logger, "Closing write stream {}", self.id);
        self.stream_closed = true;
    }

    fn update_watermark(&mut self, msg: &Message<D>) -> Result<(), WriteStreamError> {
        match msg {
            Message::TimestampedData(td) => {
                if td.timestamp < self.low_watermark {
                    return Err(WriteStreamError::TimestampError);
                }
            }
            Message::Watermark(msg_watermark) => {
                if msg_watermark < &self.low_watermark {
                    return Err(WriteStreamError::TimestampError);
                }
                self.low_watermark = msg_watermark.clone();
            }
        }
        Ok(())
    }
}

impl<D: Data> Default for WriteStream<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: Data> fmt::Debug for WriteStream<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WriteStream {{ id: {}, low_watermark: {:?} }}",
            self.id, self.low_watermark
        )
    }
}

impl<'a, D: Data + Deserialize<'a>> WriteStreamT<D> for WriteStream<D> {
    /// Specialized implementation for when the Data does not implement `Abomonation`.
    fn send(&mut self, msg: Message<D>) -> Result<(), WriteStreamError> {
        if self.stream_closed {
            return Err(WriteStreamError::Closed);
        }
        if msg.is_top_watermark() {
            self.close_stream();
        }
        self.update_watermark(&msg)?;
        let msg_arc = Arc::new(msg);

        match self.pusher.as_mut() {
            Some(pusher) => pusher.send(msg_arc).map_err(WriteStreamError::from)?,
            None => (),
        };

        // Drop pusher.
        if self.stream_closed {
            self.pusher = None;
        }
        Ok(())
    }
}
