//! Library of generic operators for building ERDOS applications.

// Private submodules
// mod join_operator;
mod filter_operator;
mod map_operator;
mod split_operator;

// Public exports
// pub use crate::dataflow::operators::join_operator::JoinOperator;
pub use filter_operator::FilterOperator;
pub use map_operator::MapOperator;
pub use split_operator::SplitOperator;
