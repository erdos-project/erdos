//! Library of ROS operators for building ERDOS applications.

// Private submodules
mod from_ros_operator;
mod to_ros_operator;

// Public exports
pub use from_ros_operator::FromRosOperator;
pub use to_ros_operator::ToRosOperator;

// Constants
const ROS_QUEUE_SIZE: usize = 1024;
