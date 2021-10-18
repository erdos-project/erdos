//! Library of generic operators for building ERDOS applications.

// Private submodules
// mod join_operator;
mod filter_operator;
mod map_operator;
mod split_operator;

// Public exports
// pub use crate::dataflow::operators::join_operator::JoinOperator;
pub use crate::dataflow::operators::filter_operator::FilterOperator;
pub use crate::dataflow::operators::map_operator::MapOperator;
pub use crate::dataflow::operators::split_operator::SplitOperator;
