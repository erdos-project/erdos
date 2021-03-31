use crate::{dataflow::stream::StreamId, node::NodeId, OperatorId};

use super::{OperatorRunner, StreamSetupHook};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Vertex {
    Driver(NodeId),
    Operator(OperatorId),
}

pub(crate) struct DriverMetadata {
    /// The id of the node on which the driver executes.
    pub id: NodeId,
    pub ingest_stream_ids: Vec<StreamId>,
    pub extract_stream_ids: Vec<StreamId>,
    pub setup_hooks: Vec<Box<dyn StreamSetupHook>>,
}

impl DriverMetadata {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            ingest_stream_ids: Vec::new(),
            extract_stream_ids: Vec::new(),
            setup_hooks: Vec::new(),
        }
    }

    pub fn add_ingest_stream<F: StreamSetupHook>(&mut self, stream_id: StreamId, setup_hook: F) {
        self.ingest_stream_ids.push(stream_id);
        self.setup_hooks.push(Box::new(setup_hook));
    }

    pub fn add_extract_stream<F: StreamSetupHook>(&mut self, stream_id: StreamId, setup_hook: F) {
        self.extract_stream_ids.push(stream_id);
        self.setup_hooks.push(Box::new(setup_hook));
    }
}

impl Clone for DriverMetadata {
    fn clone(&self) -> Self {
        let mut setup_hooks = Vec::new();
        for i in 0..self.setup_hooks.len() {
            setup_hooks.push(self.setup_hooks[i].box_clone())
        }

        Self {
            id: self.id,
            ingest_stream_ids: self.ingest_stream_ids.clone(),
            extract_stream_ids: self.extract_stream_ids.clone(),
            setup_hooks,
        }
    }
}

pub(crate) struct OperatorMetadata {
    /// The id of the operator.
    pub id: OperatorId,
    /// The name of the operator.
    pub name: Option<String>,
    /// The id of the node on which the operator executes.
    /// TODO: change this to a scheduling restriction which is an
    /// enum that may point to a node id.
    pub node_id: NodeId,
    /// The ids of the read streams the operator uses.
    pub read_stream_ids: Vec<StreamId>,
    /// The ids of the write streams the operators uses.
    pub write_stream_ids: Vec<StreamId>,
    /// Closure to be used to run the operator.
    pub runner: Box<dyn OperatorRunner>,
}

impl OperatorMetadata {
    pub fn new<F: OperatorRunner>(
        id: OperatorId,
        name: Option<String>,
        node_id: NodeId,
        read_stream_ids: Vec<StreamId>,
        write_stream_ids: Vec<StreamId>,
        runner: F,
    ) -> Self {
        Self {
            id,
            name,
            node_id,
            read_stream_ids,
            write_stream_ids,
            runner: Box::new(runner),
        }
    }
}

impl Clone for OperatorMetadata {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            node_id: self.node_id,
            read_stream_ids: self.read_stream_ids.clone(),
            write_stream_ids: self.write_stream_ids.clone(),
            runner: self.runner.box_clone(),
        }
    }
}
