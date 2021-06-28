//! Traits and implementations for executors that enable each operator to run.
//!
//! Each type of operator defined in src/dataflow/operator.rs requires a corresponding executor to
//! be implemented in this module. This executor defines how the operator handles notifications
//! from the worker channels, and invokes its corresponding callbacks upon received messages.
//!
//! TODO (Sukrit): Define how to utilize the OperatorExecutorT and OneInMessageProcessorT traits.


// Export the executors outside.
mod operator_executor;
pub use operator_executor::*;
