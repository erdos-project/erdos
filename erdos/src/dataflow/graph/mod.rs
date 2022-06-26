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
mod abstract_graph;
mod job_graph;

// Public submodules
pub(crate) mod default_graph;

// Crate-wide exports
pub(crate) use abstract_graph::AbstractGraph;
pub(crate) use job_graph::InternalGraph;
pub(crate) use job_graph::JobGraph;
use serde::{Deserialize, Serialize};

use super::{stream::StreamId, Data};

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

impl Clone for Box<dyn OperatorRunner> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}

/// Trait for functions used to set up ingest and extract streams.
pub(crate) trait StreamSetupHook:
    'static + Fn(&AbstractGraph, &mut ChannelManager) + Sync + Send
{
    fn box_clone(&self) -> Box<dyn StreamSetupHook>;
}

impl<T: 'static + Fn(&AbstractGraph, &mut ChannelManager) + Sync + Send + Clone> StreamSetupHook
    for T
{
    fn box_clone(&self) -> Box<dyn StreamSetupHook> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn StreamSetupHook> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}

/// Specifies the type of job.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Hash, Eq)]
pub(crate) enum Job {
    /// An operator in the dataflow.
    Operator(OperatorId),
    /// The driver which may interact with the dataflow.
    Driver,
}

/// A typed representation of a stream used to setup
/// and configure the dataflow graphs.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct AbstractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    id: StreamId,
    name: String,
    phantom: PhantomData<D>,
    source: Option<Job>,
    destinations: Vec<Job>,
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
            source: None,
            destinations: Vec::new(),
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
    fn get_source(&self) -> Job;
    fn get_destinations(&self) -> Vec<Job>;
    // TODO (Sukrit): These methods have been implemented as a hack
    // right now, these should be a part of the AbstractOperator once
    // these changes are tracked before the compilation to the JobGraph.
    fn register_source(&mut self, job: Job);
    fn add_destination(&mut self, job: Job);
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

    fn get_source(&self) -> Job {
        self.source.unwrap()
    }

    fn get_destinations(&self) -> Vec<Job> {
        self.destinations.clone()
    }

    fn register_source(&mut self, job: Job) {
        self.source = Some(job);
    }

    fn add_destination(&mut self, job: Job) {
        self.destinations.push(job);
    }
}

impl Clone for Box<dyn AbstractStreamT> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}

/// The [`OperatorType`] enum represents the type of operator that
/// the [`AbstractOperator`] refers to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum AbstractOperatorType {
    Source,
    ParallelSink,
    Sink,
    ParallelOneInOneOut,
    OneInOneOut,
    ParallelTwoInOneOut,
    TwoInOneOut,
    ParallelOneInTwoOut,
    OneInTwoOut,
}

/// The representation of the operator used to set up and configure the dataflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AbstractOperator {
    pub id: OperatorId,
    /// Operator configuration.
    pub config: OperatorConfig,
    /// Streams on which the operator reads.
    pub read_streams: Vec<StreamId>,
    /// Streams on which the operator writes.
    pub write_streams: Vec<StreamId>,
    /// The type of the Operator.
    pub operator_type: AbstractOperatorType,
}
