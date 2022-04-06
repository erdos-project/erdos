//! Library of generic operators for building ERDOS applications.

// Public submodules
#[cfg(feature = "ros")]
pub mod ros;

// Private submodules
// mod join_operator;
mod concat;
mod filter;
mod join;
mod map;
mod split;

// Public exports
// pub use crate::dataflow::operators::join_operator::JoinOperator;
pub use concat::{Concat, ConcatOperator};
pub use filter::{Filter, FilterOperator};
pub use join::{Join, TimestampJoinOperator};
pub use map::{FlatMapOperator, Map};
pub use split::{Split, SplitOperator};
