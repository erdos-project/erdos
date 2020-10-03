use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::dataflow::{stream::StreamId, Data, ReadStream, WriteStream};

use super::*;

/// Deadline from receipt of first message with timestamp t
/// to the sending of the last watermark(t).
pub struct TimestampOutputDeadline {
    subscribed_read_streams: HashSet<StreamId>,
    subscribed_write_streams: HashSet<StreamId>,
    start_condition_rx: Vec<broadcast::Receiver<Notification>>,
    end_condition_rx: Vec<broadcast::Receiver<Notification>>,
    max_received_timestamp: Timestamp,
    sent_watermarks: Arc<Mutex<HashMap<StreamId, Timestamp>>>,
    max_duration: Duration,
    handler: Arc<dyn Send + Sync + Fn() -> ()>,
    description: String,
}

impl TimestampOutputDeadline {
    pub fn new<F: 'static + Send + Sync + Fn() -> ()>(max_duration: Duration, handler: F) -> Self {
        Self {
            subscribed_read_streams: HashSet::new(),
            subscribed_write_streams: HashSet::new(),
            start_condition_rx: Vec::new(),
            end_condition_rx: Vec::new(),
            max_received_timestamp: Timestamp::bottom(),
            sent_watermarks: Arc::new(Mutex::new(HashMap::new())),
            max_duration,
            handler: Arc::new(handler) as Arc<dyn Send + Sync + Fn() -> ()>,
            description: "TimestampOutputDeadline".to_string(),
        }
    }

    pub fn add_read_stream<D: Data>(&mut self, read_stream: &ReadStream<D>) {
        if !self.subscribed_read_streams.contains(&read_stream.get_id()) {
            self.subscribed_read_streams.insert(read_stream.get_id());
            self.start_condition_rx.push(read_stream.subscribe());
            self.description = format!(
                "TimestampOutputDeadline on streams {:?} -> {:?}",
                self.subscribed_read_streams, self.subscribed_write_streams
            );
        }
    }

    pub fn add_write_stream<D: Data>(&mut self, write_stream: &WriteStream<D>) {
        if !self
            .subscribed_write_streams
            .contains(&write_stream.get_id())
        {
            self.subscribed_write_streams.insert(write_stream.get_id());
            self.end_condition_rx.push(write_stream.subscribe());
            self.sent_watermarks
                .try_lock()
                .unwrap()
                .insert(write_stream.get_id(), Timestamp::bottom());
            // Note that notifications can be re-ordered resulting in
            // end conditions arriving before start conditions.
            // Therefore, subscribe to the write stream in the start condition
            // to catch this reordering and prevent adding deadlines whose
            // end conditions have already been fulfilled, but not triggered.
            self.start_condition_rx.push(write_stream.subscribe());
        }
    }
}

impl Deadline for TimestampOutputDeadline {
    fn start_condition(
        &mut self,
        notification: &Notification,
    ) -> Option<(
        Instant,
        Arc<dyn Send + Sync + FnMut(&Notification) -> bool>,
        Arc<dyn Send + Sync + Fn() -> String>,
    )> {
        match &notification.notification_type {
            NotificationType::ReceivedData(_, t) | NotificationType::ReceivedWatermark(_, t) => {
                slog::warn!(
                    crate::TERMINAL_LOGGER,
                    "Got received message notification {:?}",
                    t
                );
                // Sometimes notifications are reordered.
                if t > &self.max_received_timestamp
                    && t > self
                        .sent_watermarks
                        .try_lock()
                        .unwrap()
                        .values()
                        .min()
                        .unwrap()
                {
                    self.max_received_timestamp = t.clone();
                    let deadline = notification.trigger_time + self.max_duration;
                    let start_timestamp = t.clone();
                    let sent_watermarks = self.sent_watermarks.clone();
                    let end_condition = move |n: &Notification| {
                        if let NotificationType::SentWatermark(stream_id, t) = &n.notification_type
                        {
                            // Update the hashmap of watermarks.
                            let mut sent_watermarks = sent_watermarks.try_lock().unwrap();
                            if sent_watermarks.get(stream_id).unwrap() < t {
                                sent_watermarks.insert(*stream_id, t.clone());
                            }
                            let m = sent_watermarks.values().min().unwrap();
                            slog::warn!(
                                crate::TERMINAL_LOGGER,
                                "Evaluating end condition for output deadline {:?}. Min = {:?}",
                                start_timestamp,
                                m
                            );
                            if sent_watermarks.values().min().unwrap() >= &start_timestamp {
                                return true;
                            }
                        }
                        false
                    };

                    let user_handler_clone = self.handler.clone();
                    let timestamp_str = format!("{:?}", t);
                    let handler = move || {
                        user_handler_clone();
                        timestamp_str.clone()
                    };
                    slog::warn!(crate::TERMINAL_LOGGER, "Start output deadline on {:?}.", t);
                    return Some((deadline, Arc::new(end_condition), Arc::new(handler)));
                }
            }
            NotificationType::SentWatermark(stream_id, t) => {
                slog::warn!(
                    crate::TERMINAL_LOGGER,
                    "Recieved sent watermark notification {:?}",
                    t
                );
                let mut sent_watermarks = self.sent_watermarks.try_lock().unwrap();
                if sent_watermarks.get(stream_id).unwrap() < t {
                    sent_watermarks.insert(*stream_id, t.clone());
                }
            }
            _ => (),
        };
        None
    }

    fn get_start_condition_receivers(&mut self) -> Vec<broadcast::Receiver<Notification>> {
        self.start_condition_rx.drain(..).collect()
    }

    fn get_end_condition_receivers(&mut self) -> Vec<broadcast::Receiver<Notification>> {
        self.end_condition_rx.drain(..).collect()
    }

    fn description(&self) -> &str {
        &self.description
    }
}
