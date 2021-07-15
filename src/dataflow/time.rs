use std::{cmp::Ordering, fmt::Debug};

use abomonation_derive::Abomonation;
use serde::{Deserialize, Serialize};

// Alias to [`IntTimestamp`] in case more timestamp variants are added.
pub type Timestamp = IntTimestamp;

/// Information about when an operator released a message.
#[derive(Debug, Clone, Serialize, Deserialize, Abomonation, PartialEq, Eq, Hash)]
pub enum IntTimestamp {
    /// The timestamp used to close the streams. It is the highest timestamp, and thus signifies
    /// completion of all data.
    Top,
    /// The multi-dimension timestamp conveyed by this instance.
    Time(Vec<u64>),
    /// The lowest timestamp that any stream starts with.
    Bottom,
}

impl IntTimestamp {
    pub fn is_top(&self) -> bool {
        *self == IntTimestamp::Top
    }

    pub fn is_bottom(&self) -> bool {
        *self == IntTimestamp::Bottom
    }
}

impl Ord for IntTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Top and Bottom are always equal.
            (IntTimestamp::Top, IntTimestamp::Top) => Ordering::Equal,
            (IntTimestamp::Bottom, IntTimestamp::Bottom) => Ordering::Equal,

            // Top is bigger than other timestamps.
            (IntTimestamp::Top, _) => Ordering::Greater,
            (_, IntTimestamp::Top) => Ordering::Less,

            // Bottom is less than other timestamps.
            (_, IntTimestamp::Bottom) => Ordering::Greater,
            (IntTimestamp::Bottom, _) => Ordering::Less,

            // Time should be compared lexicographically.
            (IntTimestamp::Time(a), IntTimestamp::Time(b)) => a.cmp(b),
        }
    }
}

impl PartialOrd for IntTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
