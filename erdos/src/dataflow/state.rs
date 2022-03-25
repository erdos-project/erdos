//! Structures and traits for states added to streams.

use crate::dataflow::Timestamp;
use std::collections::HashMap;

/// The [`State`] trait must be implemented by the state exposed to the operators by ERDOS.
pub trait State: 'static + Send + Sync {
    type Item: Default;

    /// The `commit` method commits the final state for a given timestamp.
    fn commit(&mut self, timestamp: &Timestamp);

    /// Retrieves the last committed timestamp by this state.
    /// This method can be used in conjunction with [`Self::at`] to retrieve the latest committed state.
    fn last_committed_timestamp(&self) -> Timestamp;

    /// Retrieve the state at a given timestamp.
    /// If the state for that timestamp hasn't been initialized yet, invoke the default method on
    /// the [`Self::Item`] type, and return the newly created state for that timestamp.
    fn at(&mut self, timestamp: &Timestamp) -> Option<&mut Self::Item>;
}

/// State implementation for () to be used by operators that are stateless.
impl State for () {
    type Item = ();

    fn commit(&mut self, _timestamp: &Timestamp) {}

    fn last_committed_timestamp(&self) -> Timestamp {
        Timestamp::Bottom
    }

    fn at(&mut self, _timestamp: &Timestamp) -> Option<&mut Self::Item> {
        None
    }
}

/// The `TimeVersionedState` provides a default implementation of the `State` for a type S.
/// The structure automatically commits the final state for a timestamp into a HashMap and
/// initializes new states for a timestamp `t` by invoking their default method.
pub struct TimeVersionedState<S>
where
    S: 'static + Default + Send + Sync,
{
    state: HashMap<Timestamp, S>,
    last_committed_timestamp: Timestamp,
}

impl<S> TimeVersionedState<S>
where
    S: 'static + Default + Send + Sync,
{
    pub fn new() -> Self {
        Self {
            state: HashMap::new(),
            last_committed_timestamp: Timestamp::Bottom,
        }
    }

    /// Evicts all committed state until and including the provided timestamp.
    pub(crate) fn evict_until(&mut self, timestamp: &Timestamp) {
        let timestamp = std::cmp::min(timestamp, &self.last_committed_timestamp);
        self.state.retain(|k, _| k > timestamp);
    }
}

impl<S> State for TimeVersionedState<S>
where
    S: 'static + Default + Send + Sync,
{
    type Item = S;

    fn commit(&mut self, timestamp: &Timestamp) {
        self.last_committed_timestamp = timestamp.clone();
    }

    fn last_committed_timestamp(&self) -> Timestamp {
        self.last_committed_timestamp.clone()
    }

    fn at(&mut self, timestamp: &Timestamp) -> Option<&mut Self::Item> {
        Some(self.state.entry(timestamp.clone()).or_default())
    }
}

/// Trait that must be implemented by a state structure that is used in a Sequential operator.
/// This state structure must implement an `append` method that enables message callbacks to add
/// intermediate state to the structure, and a `commit` method that commits the final state for a
/// given timestamp t.
pub trait AppendableState<S>: 'static + Clone + Send + Sync {
    fn append(&self, data: &S);

    fn commit(&self, timestamp: &Timestamp);

    fn last_committed_timestamp(&self) -> Timestamp;
}
