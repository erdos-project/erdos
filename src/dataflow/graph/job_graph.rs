use std::{collections::HashMap, marker::PhantomData};

use petgraph::{
    data::Build,
    stable_graph::{EdgeIndex, NodeIndex, StableGraph},
};
use serde::Deserialize;

use crate::{
    dataflow::{
        stream::{ExtractStream, IngestStream, Stream, StreamId, StreamOrigin},
        Data, LoopStream,
    },
    OperatorConfig, OperatorId,
};

use super::{OperatorRunner, StreamSetupHook};

enum Job {
    /// An operator in the dataflow.
    Operator(OperatorJob),
    /// The driver.
    Driver(DriverJob),
}

struct OperatorJob {
    id: OperatorId,
    /// Function that executes the operator.
    runner: Box<dyn OperatorRunner>,
    /// Operator configuration.
    config: OperatorConfig,
}

struct DriverJob {
    /// Used to set up ingest streams in the driver.
    setup_hooks: Vec<Box<dyn StreamSetupHook>>,
}

impl Job {
    /// Generates a new job that corresponds to an operator.
    fn new_operator<F: OperatorRunner>(id: OperatorId, runner: F, config: OperatorConfig) -> Self {
        Self::Operator(OperatorJob {
            id,
            runner: Box::new(runner),
            config,
        })
    }

    fn new_driver() -> Self {
        Self::Driver(DriverJob {
            setup_hooks: Vec::new(),
        })
    }
}

struct GraphStream<D: Data> {
    id: StreamId,
    name: String,
    origin: StreamOrigin,
    edges: Vec<EdgeIndex>,
    phantom: PhantomData<D>,
}

impl<D: Data> GraphStream<D> {
    fn new(id: StreamId, name: String, origin: StreamOrigin) -> Self {
        Self {
            id,
            name,
            origin,
            edges: Vec::new(),
            phantom: PhantomData,
        }
    }
}

trait UntypedGraphStream {
    fn id(&self) -> StreamId;
    fn name(&self) -> String;
    fn set_name(&mut self, name: String);
    fn origin(&self) -> StreamOrigin;
    fn set_origin(&mut self, origin: StreamOrigin);
    fn add_edge(&mut self, idx: EdgeIndex);
    fn get_edges(&self) -> &Vec<EdgeIndex>;
}

impl<D: Data> UntypedGraphStream for GraphStream<D> {
    fn id(&self) -> StreamId {
        self.id
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn origin(&self) -> StreamOrigin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: StreamOrigin) {
        self.origin = origin;
    }

    fn add_edge(&mut self, idx: EdgeIndex) {
        self.edges.push(idx);
    }

    fn get_edges(&self) -> &Vec<EdgeIndex> {
        &self.edges
    }
}

struct Channel {
    stream_id: StreamId,
}

impl Channel {
    fn new(stream_id: StreamId) -> Self {
        Self { stream_id }
    }
}

/// A graph of all operators, streams, and drivers in the dataflow.
/// Does not contain any scheduling information beyond constraints
/// defined in the [`OperatorConfig`].
pub(crate) struct JobGraph {
    /// Collection of jobs and connections between graphs.
    graph: StableGraph<Job, Channel>,
    /// Graph node index for the driver.
    driver_node_idx: NodeIndex,
    /// Mapping from [`OperatorId`] to node index in the graph.
    operator_id_to_idx: HashMap<OperatorId, NodeIndex>,
    /// Temporary nodes used to by loop streams.
    loop_node_idxs: HashMap<StreamId, NodeIndex>,
    /// Mapping from [`StreamId`] to structure which stores information about the stream.
    streams: HashMap<StreamId, Box<dyn UntypedGraphStream>>,
}

impl JobGraph {
    pub(crate) fn new() -> Self {
        let mut graph = StableGraph::new();
        let mut operator_id_to_idx = HashMap::new();
        let driver_node_idx = graph.add_node(Job::new_driver());

        Self {
            graph,
            driver_node_idx,
            operator_id_to_idx,
            loop_node_idxs: HashMap::new(),
            streams: HashMap::new(),
        }
    }

    /// Adds an operator and its read and write streams to the graph.
    /// Write streams are automatically named based on the operator name.
    pub(crate) fn add_operator<F: OperatorRunner, T: Data, U: Data, V: Data, W: Data>(
        &mut self,
        config: OperatorConfig,
        runner: F,
        left_read_stream: Option<&Stream<T>>,
        right_read_stream: Option<&Stream<U>>,
        left_write_stream: Option<&Stream<V>>,
        right_write_stream: Option<&Stream<W>>,
    ) {
        // Add the operator.
        let operator_id = config.id;
        let operator_name = config.get_name();
        let job = Job::new_operator(operator_id, runner, config);
        let node_idx = self.graph.add_node(job);
        self.operator_id_to_idx.insert(operator_id, node_idx);

        // Add read streams.
        if let Some(stream) = left_read_stream {
            self.connect_stream(stream.id(), node_idx);
        }
        if let Some(stream) = right_read_stream {
            self.connect_stream(stream.id(), node_idx);
        }

        // Register write streams.
        match (left_write_stream, right_write_stream) {
            (None, None) => (),
            (Some(stream), None) => {
                let stream_name = format!("{}-stream", operator_name);
                self.add_stream(stream, stream_name, StreamOrigin::Operator(operator_id));
            }
            (Some(left_write_stream), Some(right_write_stream)) => {
                let left_stream_name = format!("{}-left-stream", operator_name);
                self.add_stream(
                    left_write_stream,
                    left_stream_name,
                    StreamOrigin::Operator(operator_id),
                );
                let right_stream_name = format!("{}-right-stream", operator_name);
                self.add_stream(
                    right_write_stream,
                    right_stream_name,
                    StreamOrigin::Operator(operator_id),
                );
            }
            _ => (),
        };
    }

    pub(crate) fn add_ingest_stream<D, F: StreamSetupHook>(
        &mut self,
        ingest_stream: &IngestStream<D>,
        setup_hook: F,
    ) where
        for<'a> D: Data + Deserialize<'a>,
    {
        // Add setup hook.
        match self.graph.node_weight_mut(self.driver_node_idx) {
            Some(Job::Driver(driver)) => {
                driver.setup_hooks.push(Box::new(setup_hook));
            }
            _ => unreachable!("JobGraph could not access driver node."),
        };
        // TODO: enumerate ingest streams by starting from 0.
        let name = format!("ingest-stream-{}", ingest_stream.id());
        self.add_stream(&ingest_stream.into(), name, StreamOrigin::Driver);
    }

    pub(crate) fn add_extract_stream<D, F: StreamSetupHook>(
        &mut self,
        extract_stream: &ExtractStream<D>,
        setup_hook: F,
    ) where
        for<'a> D: Data + Deserialize<'a>,
    {
        // Add setup hook.
        match self.graph.node_weight_mut(self.driver_node_idx) {
            Some(Job::Driver(driver)) => {
                driver.setup_hooks.push(Box::new(setup_hook));
            }
            _ => unreachable!("JobGraph could not access driver node."),
        };
        self.connect_stream(extract_stream.id(), self.driver_node_idx);
    }

    pub(crate) fn add_loop_stream<D>(&mut self, loop_stream: &LoopStream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let name = format!("loop-stream-{}", loop_stream.id());
        self.add_stream(&loop_stream.into(), name, StreamOrigin::Loop);
        // Add a temporary source node to the graph.
        // let node_idx = self.graph.add_node(weight);
    }

    pub(crate) fn connect_loop<D>(&mut self, loop_stream: &LoopStream<D>, stream: &Stream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        // Transfer edges from loop to stream.
        let loop_metadata = self.streams.get(&loop_stream.id()).unwrap();
        for edge_idx in loop_metadata.get_edges().clone() {
            let (_, dest_node_idx) = self.graph.edge_endpoints(edge_idx).unwrap();
            self.graph.remove_edge(edge_idx);
            self.connect_stream(stream.id(), dest_node_idx);
        }

        // TODO: cleanup.
    }

    pub(crate) fn get_stream_name(&self, stream_id: &StreamId) -> String {
        self.streams.get(stream_id).unwrap().name()
    }

    pub(crate) fn set_stream_name(&mut self, stream_id: &StreamId, name: String) {
        self.streams.get_mut(stream_id).unwrap().set_name(name);
    }

    pub(crate) fn get_stream_origin(&self, stream_id: &StreamId) -> StreamOrigin {
        self.streams.get(stream_id).unwrap().origin()
    }

    pub(crate) fn set_stream_origin(&mut self, stream_id: &StreamId, origin: StreamOrigin) {
        self.streams.get_mut(stream_id).unwrap().set_origin(origin);
    }

    /// Helper function which converts a stream origin to the corresponding node index.
    fn stream_origin_to_node_idx(&self, origin: &StreamOrigin) -> Option<NodeIndex> {
        match origin {
            StreamOrigin::Operator(id) => self.operator_id_to_idx.get(id).cloned(),
            StreamOrigin::Driver => Some(self.driver_node_idx),
            StreamOrigin::Loop => todo!(),
        }
    }

    /// Helper function used to add new streams to the graph.
    fn add_stream<D: Data>(&mut self, stream: &Stream<D>, name: String, origin: StreamOrigin) {
        let graph_stream = GraphStream::<D>::new(stream.id(), name, origin);
        self.streams.insert(stream.id(), Box::new(graph_stream));
    }

    /// Helper function used to connect a stream to a child operator or driver.
    fn connect_stream(&mut self, stream_id: StreamId, child_node_idx: NodeIndex) {
        let origin = self.streams.get(&stream_id).unwrap().origin();
        let origin_node_idx = self.stream_origin_to_node_idx(&origin).unwrap();
        let channel = Channel::new(stream_id);
        let edge_idx = self
            .graph
            .add_edge(origin_node_idx, child_node_idx, channel);
        let stream_metadata = self.streams.get_mut(&stream_id).unwrap();
        stream_metadata.add_edge(edge_idx);
    }
}
