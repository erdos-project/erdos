use std::{sync::Arc, time::Instant};

use tokio::sync::broadcast;

use crate::dataflow::{stream::StreamId, Timestamp};

mod frequency;
mod timestamp;

pub use frequency::*;
pub use timestamp::*;

#[derive(Clone)]
pub enum NotificationType {
    ReceivedData(StreamId, Timestamp),
    ReceivedWatermark(StreamId, Timestamp),
    SentData(StreamId, Timestamp),
    SentWatermark(StreamId, Timestamp),
}

#[derive(Clone)]
pub struct Notification {
    pub(crate) trigger_time: Instant,
    pub(crate) notification_type: NotificationType,
}

impl Notification {
    pub fn new(trigger_time: Instant, notification_type: NotificationType) -> Self {
        Self {
            trigger_time,
            notification_type,
        }
    }
}

pub trait Notifier {
    fn subscribe(&self) -> broadcast::Receiver<Notification>;
}
