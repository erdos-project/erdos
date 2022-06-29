use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    node::operator_executors::OperatorExecutorT,
    scheduler::channel_manager::{ChannelManager, StreamEndpoints, StreamEndpointsT},
    OperatorConfig, OperatorId,
};

// Private submodules
#[allow(clippy::module_inception)]
mod graph;
mod job_graph;

// Crate-wide submodules
pub(crate) mod internal_graph;

// Crate-wide exports
pub use graph::Graph;
pub use internal_graph::InternalGraph;
pub(crate) use job_graph::JobGraph;
use serde::Deserialize;

use super::{stream::StreamId, Data, Stream};

/// Trait for functions that set up operator execution.
pub(crate) trait OperatorRunner:
    'static + (Fn(Arc<Mutex<ChannelManager>>) -> Box<dyn OperatorExecutorT>) + Sync + Send
{
    fn box_clone(&self) -> Box<dyn OperatorRunner>;
}

impl<
        T: 'static
            + (Fn(Arc<Mutex<ChannelManager>>) -> Box<dyn OperatorExecutorT>)
            + Sync
            + Send
            + Clone,
    > OperatorRunner for T
{
    fn box_clone(&self) -> Box<dyn OperatorRunner> {
        Box::new(self.clone())
    }
}

/// Trait for functions used to set up ingress and egress streams.
pub(crate) trait StreamSetupHook: 'static + Fn(&mut ChannelManager) + Sync + Send {
    fn box_clone(&self) -> Box<dyn StreamSetupHook>;
}

impl<T: 'static + Fn(&mut ChannelManager) + Sync + Send + Clone> StreamSetupHook for T {
    fn box_clone(&self) -> Box<dyn StreamSetupHook> {
        Box::new(self.clone())
    }
}

/// Specifies the type of job.
#[derive(Clone, Copy)]
pub(crate) enum Job {
    /// An operator in the dataflow.
    Operator(OperatorId),
    /// The driver which may interact with the dataflow.
    Driver,
}

/// A typed representation of a stream used to setup
/// and configure the dataflow graphs.
#[derive(Clone)]
pub(crate) struct AbstractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    id: StreamId,
    name: String,
    phantom: PhantomData<D>,
}

impl<D> AbstractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn new(id: StreamId, name: String) -> Self {
        Self {
            id,
            name,
            phantom: PhantomData,
        }
    }
}

impl<T, D> From<&T> for AbstractStream<D>
where
    T: Stream<D>,
    for<'a> D: Data + Deserialize<'a>,
{
    fn from(stream: &T) -> Self {
        Self {
            id: stream.id(),
            name: stream.name(),
            phantom: PhantomData,
        }
    }
}

/// A trait implemented over [`AbstractStream`]s used to preserve
/// typing while processing sets of streams.
pub(crate) trait AbstractStreamT: Send + Sync {
    fn id(&self) -> StreamId;
    fn name(&self) -> String;
    fn set_name(&mut self, name: String);
    fn box_clone(&self) -> Box<dyn AbstractStreamT>;
    fn to_stream_endpoints_t(&self) -> Box<dyn StreamEndpointsT>;
}

impl<D> AbstractStreamT for AbstractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn id(&self) -> StreamId {
        self.id
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn box_clone(&self) -> Box<dyn AbstractStreamT> {
        Box::new(self.clone())
    }

    fn to_stream_endpoints_t(&self) -> Box<dyn StreamEndpointsT> {
        Box::new(StreamEndpoints::<D>::new(self.id, self.name()))
    }
}

/// The representation of the operator used to set up and configure the dataflow.
pub(crate) struct AbstractOperator {
    pub id: OperatorId,
    /// Function that executes the operator.
    pub runner: Box<dyn OperatorRunner>,
    /// Operator configuration.
    pub config: OperatorConfig,
    /// Streams on which the operator reads.
    pub read_streams: Vec<StreamId>,
    /// Streams on which the operator writes.
    pub write_streams: Vec<StreamId>,
}

impl Clone for AbstractOperator {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            runner: self.runner.box_clone(),
            config: self.config.clone(),
            read_streams: self.read_streams.clone(),
            write_streams: self.write_streams.clone(),
        }
    }
}
