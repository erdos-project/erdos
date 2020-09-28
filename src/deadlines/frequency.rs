use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::dataflow::{Data, ReadStream};

use super::*;

pub struct TimestampReceivingFrequencyDeadline {
    max_received: Timestamp,
    max_period: Duration,
    handler: Arc<dyn Send + Sync + Fn() -> ()>,
    start_condition_receiver: Option<broadcast::Receiver<Notification>>,
    end_condition_receiver: Option<broadcast::Receiver<Notification>>,
}

impl TimestampReceivingFrequencyDeadline {
    pub fn new<F: 'static + Send + Sync + Fn() -> ()>(max_period: Duration, handler: F) -> Self {
        Self {
            max_received: Timestamp::bottom(),
            max_period,
            handler: Arc::new(handler) as Arc<dyn Send + Sync + Fn() -> ()>,
            start_condition_receiver: None,
            end_condition_receiver: None,
        }
    }

    // Should only be called once.
    pub fn subscribe<D: Data>(&mut self, read_stream: &ReadStream<D>) -> Result<(), &'static str> {
        if self.start_condition_receiver.is_some() || self.end_condition_receiver.is_some() {
            Err("Can only subscribe to one read stream!")
        } else {
            self.start_condition_receiver = Some(read_stream.subscribe());
            self.end_condition_receiver = Some(read_stream.subscribe());
            Ok(())
        }
    }
}

impl Deadline for TimestampReceivingFrequencyDeadline {
    fn start_condition(
        &mut self,
        notification: &Notification,
    ) -> Option<(
        Instant,
        Arc<dyn Send + Sync + FnMut(&Notification) -> bool>,
        Arc<dyn Send + Sync + Fn() -> ()>,
    )> {
        if let NotificationType::ReceivedWatermark(_, timestamp) = &notification.notification_type {
            if timestamp > &self.max_received {
                self.max_received = timestamp.clone();
                let physical_deadline = notification.trigger_time + self.max_period;
                let timestamp_copy = timestamp.clone();
                let end_condition = move |n: &Notification| match &n.notification_type {
                    NotificationType::ReceivedWatermark(_, t) => t > &timestamp_copy,
                    _ => false,
                };
                return Some((
                    physical_deadline,
                    Arc::new(end_condition),
                    self.handler.clone(),
                ));
            }
        }
        None
    }

    fn get_start_condition_receivers(&mut self) -> Vec<broadcast::Receiver<Notification>> {
        vec![self
            .start_condition_receiver
            .take()
            .expect("Must subscribe deadline to a ReadStream.")]
    }

    fn get_end_condition_receivers(&mut self) -> Vec<broadcast::Receiver<Notification>> {
        vec![self
            .end_condition_receiver
            .take()
            .expect("Must subscribe deadline to a ReadStream.")]
    }
}
