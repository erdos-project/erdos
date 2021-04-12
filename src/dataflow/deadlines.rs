use crate::dataflow::{stream::StreamId, time::Timestamp};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::time::Duration;

/// `CondFn` defines the type of the start and end condition functions.
/// Each function receives the `ConditionContext` that contains the information necessary for the
/// evaluation of the start and end conditions (s.a. the message counts and the state of the
/// watermarks on each stream for the given timestamp)
pub trait CondFn: Fn(&ConditionContext) -> bool + Send + Sync {}
impl<F: Fn(&ConditionContext) -> bool + Send + Sync> CondFn for F {}

pub struct DeadlineEvent {
    pub stream_id: StreamId,
    pub timestamp: Timestamp,
    pub duration: Duration,
    pub handler: Arc<dyn HandlerContextT>,
    pub end_condition: Arc<dyn CondFn>,
}

impl DeadlineEvent {
    pub fn new(
        stream_id: StreamId,
        timestamp: Timestamp,
        duration: Duration,
        handler: Arc<dyn HandlerContextT>,
        end_condition: Arc<dyn CondFn>,
    ) -> Self {
        Self {
            stream_id,
            timestamp,
            duration,
            handler,
            end_condition,
        }
    }
}

/// Enumerates the types of deadlines available to the user.
pub enum Deadline {
    TimestampDeadline(TimestampDeadline),
}

#[derive(Debug)]
pub struct ConditionContext {
    message_count: HashMap<(StreamId, Timestamp), usize>,
    watermark_status: HashMap<(StreamId, Timestamp), bool>,
}

impl ConditionContext {
    pub fn new() -> Self {
        ConditionContext {
            message_count: HashMap::new(),
            watermark_status: HashMap::new(),
        }
    }

    pub fn increment_msg_count(&mut self, stream_id: StreamId, timestamp: Timestamp) {
        let count = self
            .message_count
            .entry((stream_id, timestamp))
            .or_insert(0);
        *count += 1;
    }

    pub fn notify_watermark_arrival(&mut self, stream_id: StreamId, timestamp: Timestamp) {
        let watermark = self
            .watermark_status
            .entry((stream_id, timestamp))
            .or_insert(false);
        *watermark = true;
    }

    pub fn clear_state(&mut self, stream_id: StreamId, timestamp: Timestamp) {
        self.message_count.remove(&(stream_id, timestamp.clone()));
        self.watermark_status.remove(&(stream_id, timestamp));
    }
}

pub trait DeadlineContext: Send + Sync {
    fn calculate_deadline(&self, ctx: &ConditionContext) -> Duration;
}

pub trait HandlerContextT: Send + Sync {
    fn invoke_handler(&self, ctx: &ConditionContext);
}

pub struct TimestampDeadline {
    start_condition_fn: Arc<dyn CondFn>,
    end_condition_fn: Arc<dyn CondFn>,
    deadline_context: Box<dyn DeadlineContext>,
    handler_context: Arc<dyn HandlerContextT>,
    read_stream_ids: HashSet<StreamId>,
}

impl TimestampDeadline {
    pub fn new(
        deadline_context: impl DeadlineContext + 'static,
        handler_context: impl HandlerContextT + 'static,
    ) -> Self {
        TimestampDeadline {
            start_condition_fn: Arc::new(TimestampDeadline::default_start_condition),
            end_condition_fn: Arc::new(TimestampDeadline::default_end_condition),
            deadline_context: Box::new(deadline_context),
            handler_context: Arc::new(handler_context),
            read_stream_ids: HashSet::new(),
        }
    }

    pub fn on_read_stream(mut self, read_stream_id: StreamId) -> Self {
        self.read_stream_ids.insert(read_stream_id);
        self
    }

    pub fn with_start_condition(mut self, condition: impl 'static + CondFn) -> Self {
        self.start_condition_fn = Arc::new(condition);
        self
    }

    pub fn with_end_condition(mut self, condition: impl 'static + CondFn) -> Self {
        self.end_condition_fn = Arc::new(condition);
        self
    }

    pub(crate) fn get_start_condition_fn(&self) -> Arc<dyn CondFn> {
        Arc::clone(&self.start_condition_fn)
    }

    pub(crate) fn get_end_condition_fn(&self) -> Arc<dyn CondFn> {
        Arc::clone(&self.end_condition_fn)
    }

    pub(crate) fn start_condition(&self, condition_context: &ConditionContext) -> bool {
        (self.start_condition_fn)(condition_context)
    }

    pub(crate) fn end_condition(&self, condition_context: &ConditionContext) -> bool {
        (self.end_condition_fn)(condition_context)
    }

    pub(crate) fn calculate_deadline(&self, condition_context: &ConditionContext) -> Duration {
        self.deadline_context.calculate_deadline(condition_context)
    }

    pub(crate) fn constrained_on_read_stream(&self, read_stream_id: StreamId) -> bool {
        self.read_stream_ids.contains(&read_stream_id)
    }

    pub(crate) fn get_handler(&self) -> Arc<dyn HandlerContextT> {
        Arc::clone(&self.handler_context)
    }

    fn default_start_condition(condition_context: &ConditionContext) -> bool {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Executed default start condition with the context: {:?}",
            condition_context
        );
        true
    }

    fn default_end_condition(condition_context: &ConditionContext) -> bool {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Executed default end condition with the context: {:?}",
            condition_context
        );
        false
    }
}
