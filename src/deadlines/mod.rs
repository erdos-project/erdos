use std::{sync::Arc, time::Instant};

use tokio::sync::broadcast;

use crate::dataflow::{stream::StreamId, Timestamp};

mod frequency;
mod output;

pub use frequency::*;
pub use output::*;

#[derive(Clone)]
pub enum NotificationType {
    ReceivedData(StreamId, Timestamp),
    ReceivedWatermark(StreamId, Timestamp),
    SentData(StreamId, Timestamp),
    SentWatermark(StreamId, Timestamp),
}

#[derive(Clone)]
pub struct Notification {
    trigger_time: Instant,
    notification_type: NotificationType,
}

impl Notification {
    pub fn new(trigger_time: Instant, notification_type: NotificationType) -> Self {
        Self {
            trigger_time,
            notification_type,
        }
    }
}

pub trait Deadline {
    /// The start condition.
    /// Decides whether the notification generates a new physical time deadline.
    /// Returns a function to decide whether the deadline is made (end condition),
    /// and a handler in case the deadline is missed.
    fn start_condition(
        &mut self,
        notification: &Notification,
    ) -> Option<(
        Instant,
        Arc<dyn Send + Sync + FnMut(&Notification) -> bool>,
        Arc<dyn Send + Sync + Fn() -> ()>,
    )>;
    fn get_start_condition_receivers(&mut self) -> Vec<broadcast::Receiver<Notification>>;
    fn get_end_condition_receivers(&mut self) -> Vec<broadcast::Receiver<Notification>>;
}

pub trait Notifier {
    fn subscribe(&self) -> broadcast::Receiver<Notification>;
}
