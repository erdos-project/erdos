use std::{sync::Arc, time::SystemTime};

use tokio::sync::broadcast;

use crate::dataflow::{stream::StreamId, Timestamp};

mod frequency;

pub use frequency::TimestampReceivingFrequencyDeadline;

pub enum NotificationType {
    ReceivedData(StreamId, Timestamp),
    ReceivedWatermark(StreamId, Timestamp),
    SentData(StreamId, Timestamp),
    SentWatermark(StreamId, Timestamp),
}

pub struct Notification {
    trigger_time: SystemTime,
    notification_type: NotificationType,
}

impl Notification {
    pub fn new(trigger_time: SystemTime, notification_type: NotificationType) -> Self {
        Self {
            trigger_time,
            notification_type,
        }
    }
}

pub trait DeadlineGenerator {
    /// The start condition.
    /// Decides whether the notification generates a new physical time deadline.
    /// Returns a function to decide whether the deadline is made (end condition),
    /// and a handler in case the deadline is missed.
    fn start_condition(
        &mut self,
        notification: &Notification,
    ) -> Option<(
        SystemTime,
        Arc<dyn FnMut(&Notification) -> bool>,
        Arc<dyn Fn() -> ()>,
    )>;
    fn get_start_condition_receivers(&mut self) -> Vec<broadcast::Receiver<Notification>>;
    fn get_end_condition_receivers(&mut self) -> Vec<broadcast::Receiver<Notification>>;
}

pub trait Notifier {
    fn subscribe(&self) -> broadcast::Receiver<Notification>;
}
