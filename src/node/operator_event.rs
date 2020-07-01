use std::cmp::Ordering;
use std::fmt;

use crate::dataflow::Timestamp;

/// `OperatorEvent` is a structure that encapsulates a particular invocation of the
/// callback in response to a message or watermark. These events are processed according to the
/// partial order defined by the `PartialOrd` trait.
///
/// The event is passed to an instance of
/// [`OperatorExecutor`](../operator_executor/struct.OperatorExecutor.html)
/// which is in charge of inserting the event into a
/// [`ExecutionLattice`](../lattice/struct.ExecutionLattice.html). The `ExecutionLattice` ensures
/// that the events are processed in the partial order defined by the executor.
pub struct OperatorEvent {
    /// The timestamp of the event; timestamp of the message for regular callbacks, and timestamp of
    /// the watermark for watermark callbacks.
    pub timestamp: Timestamp,
    /// True if the callback is a watermark callback. Used to ensure that watermark callbacks are
    /// invoked after regular callbacks.
    pub is_watermark_callback: bool,
    /// The priority of the event. Smaller numbers imply higher priority.
    /// Priority currently only affects the ordering and concurrency of watermark callbacks.
    /// For two otherwise equal watermark callbacks, the lattice creates a dependency from the lower
    /// priority event to the higher priority event. Thus, these events cannot run concurrently,
    /// with the high-priority event running first. An effect is that only watermark callbacks with
    /// the same priority can run concurrently.
    pub priority: i8,
    /// The callback invoked when the event is processed.
    pub callback: Box<dyn FnOnce()>,
}

impl OperatorEvent {
    pub fn new(
        t: Timestamp,
        is_watermark_callback: bool,
        priority: i8,
        callback: impl FnOnce() + 'static,
    ) -> Self {
        Self {
            priority,
            timestamp: t,
            is_watermark_callback,
            callback: Box::new(callback),
        }
    }
}

unsafe impl Send for OperatorEvent {}

// Implement the `Display` and `Debug` traits so that we can visualize the event.
impl fmt::Display for OperatorEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Timestamp: {:?}, Watermark: {}",
            self.timestamp, self.is_watermark_callback
        )
    }
}

impl fmt::Debug for OperatorEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Timestamp: {:?}, Watermark: {}",
            self.timestamp, self.is_watermark_callback
        )
    }
}

// Implement traits to define the order in which the events should be executed.
// Make changes to the `cmp` function of the `Ord` trait to change the partial order of the events.
impl Eq for OperatorEvent {}

impl PartialEq for OperatorEvent {
    fn eq(&self, other: &OperatorEvent) -> bool {
        match self.cmp(other) {
            Ordering::Equal => true,
            _ => false,
        }
    }
}

impl Ord for OperatorEvent {
    fn cmp(&self, other: &OperatorEvent) -> Ordering {
        match (self.is_watermark_callback, other.is_watermark_callback) {
            (true, true) => {
                // Both of the events are watermarks, so the watermark with the lower timestamp
                // should run first. Ties are broken by priority where asmaller number is higher priority.
                match self.timestamp.cmp(&other.timestamp).reverse() {
                    Ordering::Equal => self.priority.cmp(&other.priority).reverse(),
                    ord => ord,
                }
            }
            (true, false) => {
                // `self` is a watermark, and `other` is a normal message callback.
                match self.timestamp.cmp(&other.timestamp) {
                    Ordering::Greater => {
                        // `self` timestamp is greater than `other`, execute `other` first.
                        Ordering::Less
                    }
                    Ordering::Equal => {
                        // `self` timestamp is equal to `other`, execute `other` first.
                        Ordering::Less
                    }
                    Ordering::Less => {
                        // `self` timestamp is less than `other`, run them in any order.
                        Ordering::Equal
                    }
                }
            }
            (false, true) => {
                // `other` is a watermark, and `self` is a normal message callback.
                match other.timestamp.cmp(&self.timestamp) {
                    Ordering::Greater => {
                        // `other` timestamp is greater than `self`, execute other first.
                        Ordering::Greater
                    }
                    Ordering::Equal => {
                        // `other` timestamp is equal to `self`, execute other first.
                        Ordering::Greater
                    }
                    Ordering::Less => {
                        // `other` timestamp is less than `self`, run them in any order.
                        Ordering::Equal
                    }
                }
            }
            (false, false) => {
                // Neither of the events are watermark callbacks, and can be run concurrently.
                Ordering::Equal
            }
        }
    }
}

impl PartialOrd for OperatorEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    /// This test ensures that two watermark messages are partially ordered based on their
    /// timestamps, and the watermark with the lower timestamp is executed first.
    #[test]
    fn test_watermark_event_orderings() {
        {
            let watermark_event_a: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![1]), true, 0, || ());
            let watermark_event_b: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![2]), true, 0, || ());
            assert_eq!(
                watermark_event_a < watermark_event_b,
                false,
                "Watermark A should not depend on Watermark B."
            );
            assert_eq!(
                watermark_event_b < watermark_event_a,
                true,
                "Watermark B should depend on Watermark A."
            );
            assert_eq!(
                watermark_event_b == watermark_event_a,
                false,
                "Watermark B should not concurrently with Watermark A."
            );
        }
        // Test that priorities should break ties only for otherwise equal watermark callbacks.
        {
            let watermark_event_a: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![1]), true, -1, || ());
            let watermark_event_b: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![1]), true, 1, || ());
            assert!(
                watermark_event_a > watermark_event_b,
                "Watermark B should depend on Watermark A"
            );

            let watermark_event_c: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![0]), true, 0, || ());
            assert!(
                watermark_event_a < watermark_event_c,
                "Watermark A should depend on Watermark C"
            );
            assert!(
                watermark_event_b < watermark_event_c,
                "Watermark B should depend on Watermark C"
            );

            let watermark_event_d: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![2]), true, 0, || ());
            assert!(
                watermark_event_d < watermark_event_a,
                "Watermark D should depend on Watermark A"
            );
            assert!(
                watermark_event_d < watermark_event_b,
                "Watermark D should depend on Watermark B"
            );

            // Priority should not affect message events
            let message_event_a: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![1]), false, 0, || ());
            assert!(
                watermark_event_a < message_event_a,
                "Watermark A with timestamp 1 should depend on Message A with timestamp 1"
            );
            assert!(
                watermark_event_b < message_event_a,
                "Watermark B with timestamp 1 should depend on Message A with timestamp 1"
            );

            let message_event_b: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![2]), false, 0, || ());
            assert_eq!(
                watermark_event_a, message_event_b,
                "Watermark A and Message A should not depend on each other"
            );
            assert_eq!(
                watermark_event_b, message_event_b,
                "Watermark A and Message A should not depend on each other"
            );
        }
    }

    /// This test ensures that two non-watermark messages are rendered equal in their partial order
    /// and thus can be run concurrently by the executor.
    #[test]
    fn test_message_event_orderings() {
        let message_event_a: OperatorEvent =
            OperatorEvent::new(Timestamp::new(vec![1]), false, 0, || ());
        let message_event_b: OperatorEvent =
            OperatorEvent::new(Timestamp::new(vec![2]), false, 0, || ());
        assert_eq!(
            message_event_a == message_event_b,
            true,
            "Message A and Message B should be able to run concurrently."
        );
        assert_eq!(
            message_event_a < message_event_b,
            false,
            "Message A and Message B should not depend on each other."
        );
        assert_eq!(
            message_event_a > message_event_b,
            false,
            "Message A and Message B should not depend on each other."
        );
    }

    #[test]
    fn test_message_watermark_event_orderings() {
        // Test that a message with a timestamp less than the watermark ensures that the watermark
        // is dependent on the message.
        {
            let message_event_a: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![1]), false, 0, || ());
            let watermark_event_b: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![2]), true, 0, || ());
            assert_eq!(
                message_event_a == watermark_event_b,
                false,
                "Message A with timestamp 1 and Watermark B\
                 with timestamp 2 should not be able to run concurrently."
            );
            assert_eq!(
                watermark_event_b == message_event_a,
                false,
                "Message A with timestamp 1 and Watermark B\
                 with timestamp 2 should not be able to run concurrently."
            );
            assert_eq!(
                message_event_a < watermark_event_b,
                false,
                "Message A with timestamp 1 should not depend\
                on Watermark B with timestamp 2."
            );
            assert_eq!(
                watermark_event_b < message_event_a,
                true,
                "Watermark B with timestamp 2 should depend\
                on Message with timestamp 1"
            );
            assert_eq!(
                message_event_a > watermark_event_b,
                true,
                "Watermark B with timestamp 2 should depend\
                on Message with timestamp 1"
            );
            assert_eq!(
                watermark_event_b > message_event_a,
                false,
                "Message A with timestamp 1 should not depend\
                on Watermark B with timestamp 2."
            );
        }

        // Test that a message with a timestamp equivalent to the watermark is run before the
        // watermark.
        {
            let message_event_a: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![1]), false, 0, || ());
            let watermark_event_b: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![1]), true, 0, || ());
            assert_eq!(
                message_event_a == watermark_event_b,
                false,
                "Message A with timestamp 1 and Watermark B\
                 with timestamp 2 should not be able to run concurrently."
            );
            assert_eq!(
                watermark_event_b == message_event_a,
                false,
                "Message A with timestamp 1 and Watermark B\
                 with timestamp 2 should not be able to run concurrently."
            );
            assert_eq!(
                message_event_a < watermark_event_b,
                false,
                "Message A with timestamp 1 should not depend\
                on Watermark B with timestamp 2."
            );
            assert_eq!(
                watermark_event_b < message_event_a,
                true,
                "Watermark B with timestamp 2 should depent\
                on Message A with timestamp 1"
            );
            assert_eq!(
                message_event_a > watermark_event_b,
                true,
                "Watermark B with timestamp 2 should depent\
                on Message A with timestamp 1"
            );
            assert_eq!(
                watermark_event_b > message_event_a,
                false,
                "Message A with timestamp 1 should not depend\
                on Watermark B with timestamp 2."
            );
        }

        // Test that a message with a timestamp greater than a watermark can be run concurrently
        // with a watermark of lesser timestamp.
        {
            let message_event_a: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![2]), false, 0, || ());
            let watermark_event_b: OperatorEvent =
                OperatorEvent::new(Timestamp::new(vec![1]), true, 0, || ());
            assert_eq!(
                message_event_a == watermark_event_b,
                true,
                "Message A with timestamp 1 and Watermark B\
                 with timestamp 2 can run concurrently."
            );
            assert_eq!(
                watermark_event_b == message_event_a,
                true,
                "Message A with timestamp 1 and Watermark B\
                 with timestamp 2 can run concurrently."
            );
            assert_eq!(
                message_event_a < watermark_event_b,
                false,
                "Message A with timestamp 1 should not depend\
                on Watermark B with timestamp 2."
            );
            assert_eq!(
                watermark_event_b < message_event_a,
                false,
                "Message A with timestamp 1 should not depend\
                on Watermark B with timestamp 2."
            );
            assert_eq!(
                message_event_a > watermark_event_b,
                false,
                "Message A with timestamp 1 should not depend\
                on Watermark B with timestamp 2."
            );
            assert_eq!(
                watermark_event_b > message_event_a,
                false,
                "Message A with timestamp 1 should not depend\
                on Watermark B with timestamp 2."
            );
        }
    }
}
