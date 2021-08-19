use std::collections::HashMap;

use petgraph::stable_graph::{EdgeIndex, NodeIndex, StableGraph};

use crate::{
    dataflow::{
        stream::{Stream, StreamId},
        Data, StreamT,
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

trait UntypedStream {
    fn id(&self) -> StreamId;
    fn name(&self) -> &str;
    fn source(&self) -> OperatorId;
}

impl<D: Data> UntypedStream for Stream<D> {
    fn id(&self) -> StreamId {
        StreamT::<D>::id(self)
    }

    // Problem: what if the user calls set_name() after?
    // Solution: Rc<RefCell> (or Arc Mutex) the Stream.
    fn name(&self) -> &str {
        StreamT::<D>::name(self)
    }

    fn source(&self) -> OperatorId {
        self.source
    }
}

struct Channel {
    stream_id: StreamId,
}

pub struct JobGraph {
    graph: StableGraph<Job, Channel>,
    operator_id_to_idx: HashMap<OperatorId, NodeIndex>,
    stream_id_to_edges: HashMap<StreamId, Vec<NodeIndex>>,
    streams: HashMap<StreamId, Box<dyn UntypedStream>>,
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
}
