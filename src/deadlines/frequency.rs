use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::dataflow::{Data, ReadStream, WriteStream};

use super::*;

pub struct TimestampReceivingFrequencyDeadline {
    max_received: Timestamp,
    max_period: Duration,
    handler: Arc<dyn Send + Sync + Fn() -> ()>,
    start_condition_receiver: Option<broadcast::Receiver<Notification>>,
    end_condition_receiver: Option<broadcast::Receiver<Notification>>,
    description: String,
}

impl TimestampReceivingFrequencyDeadline {
    pub fn new<F: 'static + Send + Sync + Fn() -> ()>(max_period: Duration, handler: F) -> Self {
        Self {
            max_received: Timestamp::bottom(),
            max_period,
            handler: Arc::new(handler) as Arc<dyn Send + Sync + Fn() -> ()>,
            start_condition_receiver: None,
            end_condition_receiver: None,
            description: "TimestampReceivingFrequencyDeadline".to_string(),
        }
    }

    // Should only be called once.
    pub fn subscribe<D: Data>(&mut self, read_stream: &ReadStream<D>) -> Result<(), &'static str> {
        if self.start_condition_receiver.is_some() || self.end_condition_receiver.is_some() {
            Err("Can only subscribe to one read stream!")
        } else {
            self.start_condition_receiver = Some(read_stream.subscribe());
            self.end_condition_receiver = Some(read_stream.subscribe());
            self.description = format!(
                "TimestampReceivingFrequencyDeadline on stream {}",
                read_stream.get_id()
            );
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
        Arc<dyn Send + Sync + Fn() -> String>,
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

                let user_handler_clone = self.handler.clone();
                let timestamp_str = format!("{:?}", timestamp);
                let handler = move || {
                    user_handler_clone();
                    timestamp_str.clone()
                };

                return Some((
                    physical_deadline,
                    Arc::new(end_condition),
                    Arc::new(handler),
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

    fn description(&self) -> &str {
        self.description.as_str()
    }
}

pub struct TimestampSendingFrequencyDeadline {
    max_sent: Timestamp,
    max_period: Duration,
    handler: Arc<dyn Send + Sync + Fn() -> ()>,
    start_condition_receiver: Option<broadcast::Receiver<Notification>>,
    end_condition_receiver: Option<broadcast::Receiver<Notification>>,
    description: String,
}

impl TimestampSendingFrequencyDeadline {
    pub fn new<F: 'static + Send + Sync + Fn() -> ()>(max_period: Duration, handler: F) -> Self {
        Self {
            max_sent: Timestamp::bottom(),
            max_period,
            handler: Arc::new(handler) as Arc<dyn Send + Sync + Fn() -> ()>,
            start_condition_receiver: None,
            end_condition_receiver: None,
            description: "TimestampSendingFrequencyDeadline".to_string(),
        }
    }

    // Should only be called once.
    pub fn subscribe<D: Data>(
        &mut self,
        write_stream: &WriteStream<D>,
    ) -> Result<(), &'static str> {
        if self.start_condition_receiver.is_some() || self.end_condition_receiver.is_some() {
            Err("Can only subscribe to one read stream!")
        } else {
            self.start_condition_receiver = Some(write_stream.subscribe());
            self.end_condition_receiver = Some(write_stream.subscribe());
            self.description = format!(
                "TimestampSendingFrequencyDeadline on stream {}",
                write_stream.get_id()
            );
            Ok(())
        }
    }
}

impl Deadline for TimestampSendingFrequencyDeadline {
    fn start_condition(
        &mut self,
        notification: &Notification,
    ) -> Option<(
        Instant,
        Arc<dyn Send + Sync + FnMut(&Notification) -> bool>,
        Arc<dyn Send + Sync + Fn() -> String>,
    )> {
        if let NotificationType::SentWatermark(_, timestamp) = &notification.notification_type {
            if timestamp > &self.max_sent {
                self.max_sent = timestamp.clone();
                let physical_deadline = notification.trigger_time + self.max_period;
                let timestamp_copy = timestamp.clone();
                let end_condition = move |n: &Notification| match &n.notification_type {
                    NotificationType::SentWatermark(_, t) => t > &timestamp_copy,
                    _ => false,
                };

                let user_handler_clone = self.handler.clone();
                let timestamp_str = format!("{:?}", timestamp);
                let handler = move || {
                    user_handler_clone();
                    timestamp_str.clone()
                };

                return Some((
                    physical_deadline,
                    Arc::new(end_condition),
                    Arc::new(handler),
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

    fn description(&self) -> &str {
        self.description.as_str()
    }
}
