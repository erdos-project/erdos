use std::cmp::Ordering;

use crate::dataflow::Timestamp;

/// Operator executors create events upon receipt of messages and watermarks. These events
/// are processed in the order of their priority and timestamp.
pub struct OperatorEvent {
    /// The priority of the event.
    pub priority: u32,
    /// The timestamp of the event; timestamp of the message for regular callbacks, and timestamp of
    /// the watermark for watermark callbacks.
    pub timestamp: Timestamp,
    /// True if the callback is a watermark callback. Used to ensure that watermark callbacks are
    /// invoked after regular callbacks.
    is_watermark_callback: bool,
    /// The callback invoked when the event is processed.
    pub callback: Box<dyn FnOnce()>,
}

impl OperatorEvent {
    pub fn new(
        t: Timestamp,
        is_watermark_callback: bool,
        callback: impl FnOnce() + 'static,
    ) -> Self {
        OperatorEvent {
            priority: 0,
            timestamp: t,
            is_watermark_callback,
            callback: Box::new(callback),
        }
    }
}

unsafe impl Send for OperatorEvent {}

// Explicitly implement trait so that the Binary heap in which OperatorEvents are
// stored becomes a min-heap instead of a max-heap.
impl Eq for OperatorEvent {}

impl PartialEq for OperatorEvent {
    fn eq(&self, other: &OperatorEvent) -> bool {
        (other.priority == self.priority)
            && (other.timestamp == self.timestamp)
            && (other.is_watermark_callback == self.is_watermark_callback)
    }
}

impl Ord for OperatorEvent {
    fn cmp(&self, other: &OperatorEvent) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.timestamp.cmp(&self.timestamp))
            .then_with(|| other.is_watermark_callback.cmp(&self.is_watermark_callback))
    }
}

impl PartialOrd for OperatorEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
