//! Library of generic operators for building ERDOS applications.

// Public submodules
#[cfg(feature = "ros")]
pub mod ros;

// Private submodules
// mod join_operator;
mod concat;
mod filter;
mod map;
mod split;

// Public exports
// pub use crate::dataflow::operators::join_operator::JoinOperator;
pub use concat::{Concat, ConcatOperator};
pub use filter::{Filter, FilterOperator};
pub use map::{Map, MapOperator};
pub use split::{Split, SplitOperator};
