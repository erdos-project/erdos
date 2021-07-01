use std::{cmp::Ordering, collections::HashSet, fmt};

use crate::{dataflow::Timestamp, Uuid};

/// `OperatorType` is an enum that enumerates the type of operators in Rust.
/// The different operator types have different execution semantics for the message and watermark
/// callbacks dictated by the `Ord` trait on the `OperatorEvent`.
pub enum OperatorType {
    ReadOnly,
    Writeable,
}

/// `OperatorEvent` is a structure that encapsulates a particular invocation of the
/// callback in response to a message or watermark. These events are processed according to the
/// partial order defined by the `PartialOrd` trait, where `x < y` implies `x` *precedes* `y`.
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
    /// IDs of items the event requires read access to.
    pub read_ids: HashSet<Uuid>,
    /// IDs of items the event requires write access to.
    pub write_ids: HashSet<Uuid>,
    /// The type of the operator for which this event is for.
    operator_type: OperatorType,
}

impl OperatorEvent {
    pub fn new(
        t: Timestamp,
        is_watermark_callback: bool,
        priority: i8,
        read_ids: HashSet<Uuid>,
        write_ids: HashSet<Uuid>,
        callback: impl FnOnce() + 'static,
        operator_type: OperatorType,
    ) -> Self {
        Self {
            priority,
            timestamp: t,
            is_watermark_callback,
            read_ids,
            write_ids,
            callback: Box::new(callback),
            operator_type,
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

// TODO: we can allow appends to occur in parallel.
/// `x < y` implies `x` *precedes* `y`.
fn resolve_access_conflicts(x: &OperatorEvent, y: &OperatorEvent) -> Ordering {
    let has_ww_conflicts = !x.write_ids.is_disjoint(&y.write_ids);
    if has_ww_conflicts {
        match x.priority.cmp(&y.priority) {
            Ordering::Equal => {
                // Prioritize events with less dependencies
                let x_num_dependencies = x.read_ids.len() + x.write_ids.len();
                let y_num_dependencies = y.read_ids.len() + y.write_ids.len();
                x_num_dependencies.cmp(&y_num_dependencies)
            }
            ord => ord,
        }
    } else {
        let has_rw_conflicts = !x.read_ids.is_disjoint(&y.write_ids);
        let has_wr_conflicts = !x.write_ids.is_disjoint(&y.read_ids);
        match (has_rw_conflicts, has_wr_conflicts) {
            (true, true) => {
                panic!("A circular dependency between OperatorEvents should never happen.")
            }
            // Prioritize writes.
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => x.priority.cmp(&y.priority),
        }
    }
}

/// Ordering used in the lattice where `self < other` implies `self` *precedes* other.
impl Ord for OperatorEvent {
    fn cmp(&self, other: &OperatorEvent) -> Ordering {
        match self.operator_type {
            OperatorType::ReadOnly => {
                match (self.is_watermark_callback, other.is_watermark_callback) {
                    (true, true) => {
                        // Both of the events are watermarks, so the watermark with the lower
                        // timestamp should run first. Ties are broken by first by dependencies to
                        // ensure that writes to state occur before reads, and then by priority
                        // where a smaller number is higher priority.
                        match self.timestamp.cmp(&other.timestamp) {
                            Ordering::Equal => match resolve_access_conflicts(&self, other) {
                                // Prioritize other.
                                Ordering::Equal => Ordering::Greater,
                                ord => ord,
                            },
                            ord => ord,
                        }
                    }
                    (true, false) => {
                        // `self` is a watermark, and `other` is a normal message callback.
                        // TODO: can compare dependencies to increase parallelism and order on a
                        // more fine-grained level.
                        match self.timestamp.cmp(&other.timestamp) {
                            Ordering::Greater => {
                                // `self` timestamp is greater than `other`, execute `other` first.
                                Ordering::Greater
                            }
                            Ordering::Equal => {
                                // `self` timestamp is equal to `other`, execute `other` first.
                                Ordering::Greater
                            }
                            Ordering::Less => {
                                // `self` timestamp is less than `other`, run them in any order.
                                // Assume state is time-versioned, so dependency issues should not
                                // arise.
                                Ordering::Equal
                            }
                        }
                    }
                    (false, true) => other.cmp(self).reverse(),
                    // Neither of the events are watermark callbacks.
                    // If they have no WW, RW, or WR conflicts, they can run concurrently.
                    (false, false) => resolve_access_conflicts(&self, other),
                }
            }
            OperatorType::Writeable => {
                match (self.is_watermark_callback, other.is_watermark_callback) {
                    (true, true) => {
                        // Both of the events are watermarks, so the watermark with the lower
                        // timestamp should run first. If equal, break ties by running `self`.
                        match self.timestamp.cmp(&other.timestamp) {
                            Ordering::Equal => Ordering::Less,
                            ord => ord,
                        }
                    }
                    (true, false) => {
                        // `self` is a watermark, and `other` is a normal message callback.
                        match self.timestamp.cmp(&other.timestamp) {
                            Ordering::Greater => {
                                // `self` timestamp is greater than `other`, execute `other` first.
                                Ordering::Greater
                            }
                            Ordering::Equal => {
                                // `self` timestamp is equal to `other`, execute `other` first.
                                Ordering::Greater
                            }
                            Ordering::Less => {
                                // `self` timestamp is less than `other`, execute `self` first.
                                Ordering::Less
                            }
                        }
                    }
                    (false, true) => {
                        // `self` is a message callback, and `other` is a watermark callback.
                        match self.timestamp.cmp(&other.timestamp) {
                            Ordering::Greater => {
                                // `self` timestamp is greater than `other`, execute `other` first.
                                Ordering::Greater
                            }
                            Ordering::Equal => {
                                // `self` timestamp is equal to `other`, execute `self` first.
                                Ordering::Less
                            }
                            Ordering::Less => {
                                // `self` timestamp is less than `other`, execute `self` first.
                                Ordering::Less
                            }
                        }
                    }
                    (false, false) => {
                        // Both `self` and `other` are message callbacks, execute the one with a
                        // lower timestamp first, and break ties by prioritizing `self`.
                        match self.timestamp.cmp(&other.timestamp) {
                            Ordering::Greater => {
                                // `self` timestamp is greater than `other`, execute `other` first.
                                Ordering::Greater
                            }
                            Ordering::Equal => {
                                // The timestamps are equal, prioritize `self`.
                                Ordering::Less
                            }
                            Ordering::Less => {
                                // `self` timestamp is less than `other`, execute `self` first.
                                Ordering::Less
                            }
                        }
                    }
                }
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
            let watermark_event_a: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![1]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            let watermark_event_b: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![2]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert!(
                watermark_event_a < watermark_event_b,
                "A has a lower timestamp and should precede B."
            );
        }
        // Test that priorities should break ties only for otherwise equal watermark callbacks.
        {
            let watermark_event_a: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![1]),
                true,
                -1,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            let watermark_event_b: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![1]),
                true,
                1,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert!(
                watermark_event_a < watermark_event_b,
                "A is higher priority and should precede B."
            );

            let watermark_event_c: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![0]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert!(
                watermark_event_a > watermark_event_c,
                "C has a smaller timestamp and should precede A."
            );
            assert!(
                watermark_event_b > watermark_event_c,
                "C has a smaller timestamp and should precede B."
            );

            let watermark_event_d: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![2]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert!(
                watermark_event_a < watermark_event_d,
                "D has a larger timestamp and should follow A."
            );
            assert!(
                watermark_event_b < watermark_event_d,
                "D has a larger timestamp and should follow B."
            );

            // Priority should not affect message events
            let message_event_a: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert!(
                message_event_a < watermark_event_a,
                "Message A should precede Watermark A independent of priority."
            );
            assert!(
                message_event_a < watermark_event_b,
                "Message A should precede Watermark B independent of priority."
            );

            let message_event_b: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![2]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert_eq!(
                watermark_event_a, message_event_b,
                "Watermark A and Message B can execute concurrently."
            );
            assert_eq!(
                watermark_event_b, message_event_b,
                "Watermark B and Message B can execute concurrently."
            );
        }
    }

    /// This test ensures that two non-watermark messages are rendered equal in their partial order
    /// and thus can be run concurrently by the executor.
    #[test]
    fn test_message_event_orderings() {
        let message_event_a: OperatorEvent = OperatorEvent::new(
            Timestamp::Time(vec![1]),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            || (),
        );
        let message_event_b: OperatorEvent = OperatorEvent::new(
            Timestamp::Time(vec![2]),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            || (),
        );
        assert!(
            message_event_a == message_event_b,
            "Message A and Message B can run concurrently."
        );
    }

    #[test]
    fn test_message_watermark_event_orderings() {
        // Test that a message with a timestamp less than the watermark ensures that the watermark
        // is dependent on the message.
        {
            let message_event_a: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            let watermark_event_b: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![2]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert!(
                message_event_a < watermark_event_b,
                "Message A with timestamp 1 should precede Watermark B with timestamp 2."
            );
        }

        // Test that a message with a timestamp equivalent to the watermark is run before the
        // watermark.
        {
            let message_event_a: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            let watermark_event_b: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![1]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert!(
                message_event_a < watermark_event_b,
                "Message A with timestamp 1 should precede Watermark B with timestamp 1."
            );
        }

        // Test that a message with a timestamp greater than a watermark can be run concurrently
        // with a watermark of lesser timestamp.
        {
            let message_event_a: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![2]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            let watermark_event_b: OperatorEvent = OperatorEvent::new(
                Timestamp::Time(vec![1]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            );
            assert!(
                message_event_a == watermark_event_b,
                "Message A with timestamp 1 and Watermark B with timestamp 2 can run concurrently."
            );
        }
    }

    #[test]
    fn test_resolve_access_conflicts() {
        let mut write_ids = HashSet::new();
        write_ids.insert(Uuid::new_deterministic());
        let event_a = OperatorEvent::new(
            Timestamp::Time(vec![0]),
            true,
            0,
            HashSet::new(),
            write_ids.clone(),
            || {},
        );

        let event_b = OperatorEvent::new(
            Timestamp::Time(vec![0]),
            true,
            1,
            HashSet::new(),
            write_ids.clone(),
            || {},
        );
        assert!(event_a < event_b, "A should precede B due to priority.");

        let mut read_ids = HashSet::new();
        read_ids.insert(Uuid::new_deterministic());
        let event_c = OperatorEvent::new(
            Timestamp::Time(vec![0]),
            true,
            0,
            read_ids,
            write_ids.clone(),
            || {},
        );
        assert!(
            event_a < event_c,
            "A should precede C because A has fewer dependencies."
        );

        let read_ids = write_ids.clone();
        let event_d = OperatorEvent::new(
            Timestamp::Time(vec![0]),
            true,
            0,
            read_ids,
            HashSet::new(),
            || {},
        );
        assert!(
            event_a < event_d,
            "A should precede D due to a WR conflict."
        );
    }
}
