use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    communication::ControlMessage, // node::operator_executor::OperatorExecutor,
    scheduler::channel_manager::ChannelManager,
};

// Dummy struct to make things compile.
pub struct OperatorExecutor {}
impl OperatorExecutor {
    pub async fn execute(&mut self) {}
}

// Private submodules
mod edge;
mod graph;
mod vertex;

// Public submodules
pub mod default_graph;

// Crate-wide exports
pub(crate) use edge::{Channel, ChannelMetadata, StreamMetadata};
pub(crate) use vertex::{DriverMetadata, OperatorMetadata, Vertex};

// Public exports
pub use graph::Graph;

pub trait OperatorRunner:
    'static
    + (Fn(
        Arc<Mutex<ChannelManager>>,
        UnboundedSender<ControlMessage>,
        UnboundedReceiver<ControlMessage>,
    ) -> OperatorExecutor)
    + Sync
    + Send
{
    fn box_clone(&self) -> Box<dyn OperatorRunner>;
}

impl<
        T: 'static
            + (Fn(
                Arc<Mutex<ChannelManager>>,
                UnboundedSender<ControlMessage>,
                UnboundedReceiver<ControlMessage>,
            ) -> OperatorExecutor)
            + Sync
            + Send
            + Clone,
    > OperatorRunner for T
{
    fn box_clone(&self) -> Box<dyn OperatorRunner> {
        Box::new(self.clone())
    }
}

pub trait StreamSetupHook: 'static + Fn(Arc<Mutex<ChannelManager>>) + Sync + Send {
    fn box_clone(&self) -> Box<dyn StreamSetupHook>;
}

impl<T: 'static + Fn(Arc<Mutex<ChannelManager>>) + Sync + Send + Clone> StreamSetupHook for T {
    fn box_clone(&self) -> Box<dyn StreamSetupHook> {
        Box::new(self.clone())
    }
}
