use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use super::*;
use crate::dataflow::{Data, ReadStream, WriteStream};

pub struct TimestampDeadline {
    pub(crate) duration: Duration,
    pub(crate) handler: Option<Arc<dyn Send + Sync + Fn(Timestamp) -> ()>>,
    pub(crate) read_stream_notifications: Option<broadcast::Receiver<Notification>>,
    pub(crate) write_stream_notifications: Option<broadcast::Receiver<Notification>>,
    pub(crate) read_stream_id: Option<StreamId>,
    pub(crate) write_stream_id: Option<StreamId>,
}

impl TimestampDeadline {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            handler: None,
            read_stream_notifications: None,
            write_stream_notifications: None,
            read_stream_id: None,
            write_stream_id: None,
        }
    }

    pub fn with_handler<F: 'static + Send + Sync + Fn(Timestamp) -> ()>(
        mut self,
        handler: F,
    ) -> Self {
        self.handler = Some(Arc::new(handler));
        self
    }

    pub fn on_read_stream<D: Data>(mut self, read_stream: &ReadStream<D>) -> Self {
        self.read_stream_notifications = Some(read_stream.subscribe());
        self.read_stream_id = Some(read_stream.get_id());
        self
    }

    pub fn on_write_stream<D: Data>(mut self, write_stream: &WriteStream<D>) -> Self {
        self.write_stream_notifications = Some(write_stream.subscribe());
        self.write_stream_id = Some(write_stream.get_id());
        self
    }
}
