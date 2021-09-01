use std::{collections::HashMap, marker::PhantomData};

use petgraph::stable_graph::{EdgeIndex, NodeIndex, StableGraph};

use crate::{
    dataflow::{
        stream::{Stream, StreamId, StreamOrigin},
        Data,
    },
    OperatorConfig, OperatorId,
};

use super::{OperatorRunner, StreamSetupHook};

struct Job {
    id: OperatorId,
    /// Used to set up ingest streams in the driver.
    setup_hooks: Vec<Box<dyn StreamSetupHook>>,
    /// Function that executes the operator.
    runner: Box<dyn OperatorRunner>,
    /// Operator configuration.
    config: OperatorConfig,
}

struct GraphStream<D: Data> {
    id: StreamId,
    origin: StreamOrigin,
    name: String,
    phantom: PhantomData<D>,
}

trait UntypedGraphStream {
    fn id(&self) -> StreamId;
    fn origin(&self) -> StreamOrigin;
    fn set_origin(&mut self, origin: StreamOrigin);
    fn name(&self) -> String;
    fn set_name(&mut self, name: String);
}

impl<D: Data> UntypedGraphStream for GraphStream<D> {
    fn id(&self) -> StreamId {
        self.id
    }

    fn origin(&self) -> StreamOrigin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: StreamOrigin) {
        self.origin = origin;
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn name(&self) -> String {
        self.name.clone()
    }
}

struct Channel {
    stream_id: StreamId,
}

pub struct JobGraph {
    graph: StableGraph<Job, Channel>,
    operator_id_to_idx: HashMap<OperatorId, NodeIndex>,
    stream_id_to_edges: HashMap<StreamId, Vec<NodeIndex>>,
    streams: HashMap<StreamId, Box<dyn UntypedGraphStream>>,
}

impl JobGraph {
    pub(crate) fn add_operator<F: OperatorRunner, T: Data, U: Data, V: Data, W: Data>(
        config: OperatorConfig,
        runner: F,
        left_read_stream: Option<Stream<T>>,
        right_read_stream: Option<Stream<U>>,
        left_write_stream: Option<Stream<V>>,
        right_write_stream: Option<Stream<W>>,
    ) {
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
}
