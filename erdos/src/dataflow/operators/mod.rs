//! Library of generic operators for building ERDOS applications.

// Public submodules
#[cfg(feature = "ros")]
pub mod ros;

// Private submodules
// mod join_operator;
mod concat;
mod filter_operator;
mod map_operator;
mod split_operator;

// Public exports
// pub use crate::dataflow::operators::join_operator::JoinOperator;
pub use concat::{Concat, ConcatOperator};
pub use filter_operator::FilterOperator;
pub use map_operator::MapOperator;
pub use split_operator::SplitOperator;
