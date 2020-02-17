use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    communication::ControlMessage, node::operator_executor::OperatorExecutor,
    scheduler::channel_manager::ChannelManager,
};

pub mod default_graph;
pub mod edge;
pub mod graph;
pub mod vertex;

pub use edge::{Channel, ChannelMetadata, StreamMetadata};
pub use graph::Graph;
pub use vertex::{DriverMetadata, OperatorMetadata, Vertex};

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
