use crate::dataflow::{stream::StreamId, time::Timestamp, StateT};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::time::Duration;

/// The identifier assigned to each deadline installed in a SetupContext.
pub type DeadlineId = crate::Uuid;

/// `CondFn` defines the type of the start and end condition functions.
/// Each function receives the `ConditionContext` that contains the information necessary for the
/// evaluation of the start and end conditions (s.a. the message counts and the state of the
/// watermarks on each stream for the given timestamp) and the current timestamp for which the
/// condition is being executed.
pub trait CondFn: Fn(&ConditionContext, &Timestamp) -> bool + Send + Sync {}
impl<F: Fn(&ConditionContext, &Timestamp) -> bool + Send + Sync> CondFn for F {}

/// A trait that defines the deadline function. This function receives access to the State of the
/// operator along with the current timestamp, and must calculate the time after which the deadline
/// expires.
pub trait DeadlineFn<S>: FnMut(&S, &Timestamp) -> Duration + Send + Sync {}
impl<S, F: FnMut(&S, &Timestamp) -> Duration + Send + Sync> DeadlineFn<S> for F {}

/// A trait that defines the handler function, which is invoked in the case of a missed deadline.
pub trait HandlerFn<S>: FnMut(&S, &Timestamp) + Send + Sync {}
impl<S, F: FnMut(&S, &Timestamp) + Send + Sync> HandlerFn<S> for F {}

/// A trait implemented by the different types of deadlines available to operators. 
pub trait DeadlineT<S>: Send + Sync {
    fn is_constrained_on_read_stream(&self, stream_id: StreamId) -> bool;

    fn invoke_start_condition(
        &self,
        condition_context: &ConditionContext,
        timestamp: &Timestamp,
    ) -> bool;

    fn calculate_deadline(&self, state: &S, timestamp: &Timestamp) -> Duration;

    fn get_end_condition_fn(&self) -> Arc<dyn CondFn>;

    fn get_id(&self) -> DeadlineId;

    fn invoke_handler(&self, state: &mut S, timestamp: &Timestamp);
}

/// A TimestampDeadline constrains the duration between the start and end conditions for a
/// particular timestamp.
pub struct TimestampDeadline<S>
where
    S: StateT,
{
    start_condition_fn: Arc<dyn CondFn>,
    end_condition_fn: Arc<dyn CondFn>,
    deadline_fn: Arc<Mutex<dyn DeadlineFn<S>>>,
    handler_fn: Arc<Mutex<dyn HandlerFn<S>>>,
    read_stream_ids: HashSet<StreamId>,
    id: DeadlineId,
}

#[allow(dead_code)]
impl<S> TimestampDeadline<S>
where
    S: StateT,
{
    pub fn new(
        deadline_fn: impl DeadlineFn<S> + 'static,
        handler_fn: impl HandlerFn<S> + 'static,
    ) -> Self {
        Self {
            start_condition_fn: Arc::new(TimestampDeadline::<S>::default_start_condition),
            end_condition_fn: Arc::new(TimestampDeadline::<S>::default_end_condition),
            deadline_fn: Arc::new(Mutex::new(deadline_fn)),
            handler_fn: Arc::new(Mutex::new(handler_fn)),
            read_stream_ids: HashSet::new(),
            id: DeadlineId::new_deterministic(),
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

    pub(crate) fn end_condition(
        &self,
        condition_context: &ConditionContext,
        current_timestamp: &Timestamp,
    ) -> bool {
        (self.end_condition_fn)(condition_context, current_timestamp)
    }

    /// The default start condition of TimestampDeadlines.
    fn default_start_condition(
        condition_context: &ConditionContext,
        current_timestamp: &Timestamp,
    ) -> bool {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Executed default start condition for the timestamp: {:?} with the context: {:?}",
            current_timestamp,
            condition_context,
        );
        // TODO (Sukrit): Implement the start condition function.
        true
    }

    /// The default end condition of TimestampDeadlines.
    fn default_end_condition(
        condition_context: &ConditionContext,
        current_timestamp: &Timestamp,
    ) -> bool {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Executed default end condition for the timestamp: {:?} with the context: {:?}",
            current_timestamp,
            condition_context,
        );
        // TODO (Sukrit): Implement the end condition function.
        false
    }
}

impl<S> DeadlineT<S> for TimestampDeadline<S>
where
    S: StateT,
{
    fn is_constrained_on_read_stream(&self, stream_id: StreamId) -> bool {
        self.read_stream_ids.contains(&stream_id)
    }

    fn invoke_start_condition(
        &self,
        condition_context: &ConditionContext,
        timestamp: &Timestamp,
    ) -> bool {
        (self.start_condition_fn)(condition_context, timestamp)
    }

    fn calculate_deadline(&self, state: &S, timestamp: &Timestamp) -> Duration {
        (self.deadline_fn.lock().unwrap())(state, timestamp)
    }

    fn get_end_condition_fn(&self) -> Arc<dyn CondFn> {
        Arc::clone(&self.end_condition_fn)
    }

    fn get_id(&self) -> DeadlineId {
        self.id
    }

    fn invoke_handler(&self, state: &mut S, timestamp: &Timestamp) {
        (self.handler_fn.lock().unwrap())(state, timestamp)
    }
}

/// A `DeadlineEvent` structure defines a deadline that is generated upon the fulfillment of a
/// start condition on a given stream and a given timestamp (we assume a single deadline for each
/// timestamp). Upon expiration of the deadline (defined as `duration`), the `end_condition`
/// function is checked, and the `handler` is invoked if it was not satisfied.
pub struct DeadlineEvent {
    pub stream_id: StreamId,
    pub timestamp: Timestamp,
    pub duration: Duration,
    pub end_condition: Arc<dyn CondFn>,
    pub id: DeadlineId,
}

impl DeadlineEvent {
    pub fn new(
        stream_id: StreamId,
        timestamp: Timestamp,
        duration: Duration,
        end_condition: Arc<dyn CondFn>,
        id: DeadlineId,
    ) -> Self {
        Self {
            stream_id,
            timestamp,
            duration,
            end_condition,
            id,
        }
    }
}

/// A ConditionContext contains the count of the number of messages received on a stream along with
/// the status of the watermark for each timestamp. This data is made available to the start and
/// end condition functions to decide when to trigger the beginning and completion of deadlines.
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
