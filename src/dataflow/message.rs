use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Trait for valid message data. The data must be clonable, sendable between threads and
/// serializable.
pub trait Data: 'static + Clone + Send + Debug + Serialize {}
/// Any type that is clonable, sendable, and can be serialized and dereserialized implements `Data`.
impl<T> Data for T where for<'a> T: 'static + Clone + Send + Debug + Serialize + Deserialize<'a> {}

/// Operators send messages on streams. A message can be either a `Watermark` or a `TimestampedData`.
#[derive(Clone, Debug, Serialize, Deserialize, Abomonation)]
pub enum Message<D: Data> {
    TimestampedData(TimestampedData<D>),
    Watermark(Timestamp),
}

impl<D: Data> Message<D> {
    /// Creates a new `TimestampedData` message.
    pub fn new_message(timestamp: Timestamp, data: D) -> Message<D> {
        Self::TimestampedData(TimestampedData::new(timestamp, data))
    }

    /// Creates a new `Watermark` message.
    pub fn new_watermark(timestamp: Timestamp) -> Message<D> {
        Self::Watermark(timestamp)
    }
}

impl<D: Data + PartialEq> PartialEq for Message<D> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::TimestampedData(d1), Self::TimestampedData(d2)) => d1 == d2,
            (Self::Watermark(w1), Self::Watermark(w2)) => w1 == w2,
            _ => false,
        }
    }
}

/// Data message which operators send along streams.
#[derive(Debug, Clone, Serialize, Deserialize, Abomonation)]
pub struct TimestampedData<D: Data> {
    /// Timestamp of the message.
    pub timestamp: Timestamp,
    /// Data is an option in case one wants to send null messages.
    pub data: D,
}

impl<D: Data> TimestampedData<D> {
    pub fn new(timestamp: Timestamp, data: D) -> Self {
        Self { timestamp, data }
    }
}

impl<D: Data + PartialEq> PartialEq for TimestampedData<D> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.data == other.data
    }
}

// Alias timestamp in case we move to generic timestamps.
pub type Timestamp = IntTimestamp;

/// Information about when an operator released a message.
#[derive(Debug, Clone, Serialize, Deserialize, Abomonation, PartialOrd, PartialEq, Ord, Eq)]
pub struct IntTimestamp {
    // TODO: Storing the dimensions in a vector is inefficient. Fix.
    /// Stores the timestamp values for each dimension.
    pub time: Vec<u64>,
}

impl IntTimestamp {
    pub fn new(time: Vec<u64>) -> Self {
        Self { time }
    }
}
