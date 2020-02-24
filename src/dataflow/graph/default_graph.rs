use std::cell::RefCell;

use serde::Deserialize;

use crate::{
    dataflow::{
        stream::{ExtractStream, IngestStream, LoopStream, StreamId, WriteStream},
        Data,
    },
    node::NodeId,
    OperatorId,
};

use super::{Graph, OperatorRunner, StreamSetupHook};

thread_local!(static DEFAULT_GRAPH: RefCell<Graph> = RefCell::new(Graph::new()));

/// Adds an operator to the default graph.
///
/// The operator is pinned on a given node.
pub fn add_operator<F: OperatorRunner>(
    id: OperatorId,
    name: Option<String>,
    node_id: NodeId,
    read_stream_ids: Vec<StreamId>,
    write_stream_ids: Vec<StreamId>,
    runner: F,
) {
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut()
            .add_operator(id, name, node_id, read_stream_ids, write_stream_ids, runner);
    });
}

pub fn add_operator_stream<D>(operator_id: OperatorId, write_stream: &WriteStream<D>)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut()
            .add_operator_stream(operator_id, write_stream);
    });
}

pub fn add_ingest_stream<D, F: StreamSetupHook>(ingest_stream: &IngestStream<D>, setup_hook: F)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut().add_ingest_stream(ingest_stream, setup_hook);
    });
}

pub fn add_extract_stream<D, F: StreamSetupHook>(extract_stream: &ExtractStream<D>, setup_hook: F)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut()
            .add_extract_stream(extract_stream, setup_hook);
    });
}

pub fn add_loop_stream<D>(loop_stream: &LoopStream<D>)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut().add_loop_stream(loop_stream);
    });
}

/// Adds an alias from from_id to to_id on the default graph.
pub fn add_stream_alias(from_id: StreamId, to_id: StreamId) -> Result<(), String> {
    DEFAULT_GRAPH.with(|g| g.borrow_mut().add_stream_alias(from_id, to_id))
}

pub fn clone() -> Graph {
    DEFAULT_GRAPH.with(|g| g.borrow().clone())
}

/// Updates the graph, and returns previous value
pub fn set(graph: Graph) -> Graph {
    DEFAULT_GRAPH.with(|g| g.replace(graph))
}
