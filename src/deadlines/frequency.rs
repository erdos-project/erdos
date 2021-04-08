use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::dataflow::{Data, ReadStream, WriteStream};

use super::*;

pub struct ReceivingFrequencyDeadline {
    pub(crate) duration: Duration,
    pub(crate) handler: Option<Arc<dyn Send + Sync + Fn(Timestamp) -> ()>>,
    pub(crate) notification_rx: Option<broadcast::Receiver<Notification>>,
    pub(crate) read_stream_id: Option<StreamId>,
}

impl ReceivingFrequencyDeadline {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            handler: None,
            notification_rx: None,
            read_stream_id: None,
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
        self.notification_rx = Some(read_stream.subscribe());
        self.read_stream_id = Some(read_stream.get_id());
        self
    }
}
