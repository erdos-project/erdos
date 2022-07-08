use std::{
    error::Error,
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    communication::data_plane::{StreamEndpoints, StreamEndpointsT, StreamManager},
    node::operator_executors::OperatorExecutorT,
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
pub(crate) use internal_graph::InternalGraph;
pub(crate) use job_graph::{AbstractJobGraph, JobGraph};
use serde::{Deserialize, Serialize};

use super::{stream::StreamId, Data, Stream};

/// The error raised when the compilation of a [`Graph`] fails.
#[derive(Debug, Clone)]
pub struct GraphCompilationError(String);

impl Error for GraphCompilationError {}

impl fmt::Display for GraphCompilationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphCompilationError({})", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JobGraphId(String);

/// Trait for functions that set up the execution of a [`Job`].
pub(crate) trait JobRunner:
    'static + (Fn(Arc<Mutex<StreamManager>>) -> Option<Box<dyn OperatorExecutorT>>) + Sync + Send
{
    fn box_clone(&self) -> Box<dyn JobRunner>;
}

impl<
        T: 'static
            + (Fn(Arc<Mutex<StreamManager>>) -> Option<Box<dyn OperatorExecutorT>>)
            + Sync
            + Send
            + Clone,
    > JobRunner for T
{
    fn box_clone(&self) -> Box<dyn JobRunner> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn JobRunner> {
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
    /// The ID of the Stream represented by this `AbstractStream`.
    id: StreamId,
    /// The name of the Stream represented by this `AbstractStream`.
    name: String,
    /// The [`Job`] from where the data for this Stream is generated.
    source: Option<Job>,
    /// A collection of [`Job`]s that consume the data generated by this Stream.
    destinations: Vec<Job>,
    phantom: PhantomData<D>,
}

impl<D> AbstractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// Construct an `AbstractStream` representation of a Stream with the
    /// given `id` and `name`.
    fn new(id: StreamId, name: String) -> Self {
        Self {
            id,
            name,
            source: None,
            destinations: Vec::new(),
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
            source: None,
            destinations: Vec::new(),
            phantom: PhantomData,
        }
    }
}

/// A trait implemented over [`AbstractStream`]s used to preserve
/// typing while processing sets of streams.
pub(crate) trait AbstractStreamT: Send + Sync {
    /// Get the ID of the Stream represented by this [`AbstractStream`] instance.
    fn id(&self) -> StreamId;

    /// Get the name of the Stream represented by this [`AbstractStream`] instance.
    fn name(&self) -> String;

    /// Get the [`Job`] from where the data for this Stream is generated.
    ///
    /// An [`AbstractStream`] should always have a `source` post-compilation of the
    /// [`Graph`] into a [`JobGraph`].
    fn source(&self) -> Option<Job>;

    /// Get the set of [`Job`]s that consume the data generated by this Stream.
    fn destinations(&self) -> Vec<Job>;

    /// Get a [`Box`] cloned version of the underlying [`AbstractStream`] instance.
    fn box_clone(&self) -> Box<dyn AbstractStreamT>;

    fn to_stream_endpoints_t(&self) -> Box<dyn StreamEndpointsT>;

    /// Register the source [`Job`] that generates the data for this Stream.
    ///
    /// This method is used in the compilation stage to ensure that every Stream has
    /// a source [`Job`].
    fn register_source(&mut self, job: Job);

    /// Add a destination [`Job`] that consumes the generated data to this Stream.
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

    fn source(&self) -> Option<Job> {
        self.source
    }

    fn destinations(&self) -> Vec<Job> {
        self.destinations.clone()
    }

    fn box_clone(&self) -> Box<dyn AbstractStreamT> {
        Box::new(self.clone())
    }

    fn to_stream_endpoints_t(&self) -> Box<dyn StreamEndpointsT> {
        Box::new(StreamEndpoints::<D>::new(self.id(), self.name()))
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

impl fmt::Debug for dyn AbstractStreamT {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AbstractStream {} (ID={})", self.name(), self.id())
    }
}

/// An enum representing the type of the operator that teh [`AbstractOperator`] refers to.
///
/// See the [`operator`](crate::dataflow::operator) module for the corresponding traits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum AbstractOperatorType {
    /// A variant representing an operator that implements the
    /// [`Source`](crate::dataflow::operator::Source) trait.
    Source,
    /// A variant representing an operator that implements the
    /// [`ParallelSink`](crate::dataflow::operator::ParallelSink) trait.
    ParallelSink,
    /// A variant representing an operator that implements the
    /// [`Sink`](crate::dataflow::operator::Sink) trait.
    Sink,
    /// A variant representing an operator that implements the
    /// [`ParallelOneInOneOut`](crate::dataflow::operator::ParallelOneInOneOut) trait.
    ParallelOneInOneOut,
    /// A variant representing an operator that implements the
    /// [`OneInOneOut`](crate::dataflow::operator::OneInOneOut) trait.
    OneInOneOut,
    /// A variant representing an operator that implements the
    /// [`ParallelTwoInOneOut`](crate::dataflow::operator::ParallelTwoInOneOut) trait.
    ParallelTwoInOneOut,
    /// A variant representing an operator that implements the
    /// [`TwoInOneOut`](crate::dataflow::operator::TwoInOneOut) trait.
    TwoInOneOut,
    /// A variant representing an operator that implements the
    /// [`ParallelOneInTwoOut`](crate::dataflow::operator::ParallelOneInTwoOut) trait.
    ParallelOneInTwoOut,
    /// A variant representing an operator that implements the
    /// [`OneInTwoOut`](crate::dataflow::operator::OneInTwoOut) trait.
    OneInTwoOut,
}

/// An abstract representation of each of the [`Operator`](crate::dataflow::operator) in a
/// dataflow graph.
///
/// This structure is communicated by a [`Worker`](crate::node::WorkerNode) to a
/// [`Leader`](crate::node::Leader) in order to enable the scheduling of the
/// [`JobGraph`](crate::dataflow::graph::JobGraph).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AbstractOperator {
    /// The ID of the `Operator` that this instance abstracts.
    pub id: OperatorId,
    /// The configuration of the `Operator` that this instance abstracts.
    pub config: OperatorConfig,
    /// The IDs of the [`ReadStream`](crate::dataflow::stream::ReadStream)s from where the
    /// `Operator` underlying this instance retrieves its data from.
    pub read_streams: Vec<StreamId>,
    /// The IDs of the [`WriteStream`](crate::dataflow::stream::WriteStream)s to where the
    /// `Operator` underlying this instance sends its output data.
    pub write_streams: Vec<StreamId>,
    /// The type of [trait](crate::dataflow::operator) implemented by the `Operator`
    /// that this instance abstracts.
    pub operator_type: AbstractOperatorType,
}
