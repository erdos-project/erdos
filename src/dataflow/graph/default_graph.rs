//! A globally accessible dataflow graph.
//!
//! This is used in the driver when connecting new operators,
//! or setting up [`IngestStream`]s, [`ExtractStream`]s, and [`LoopStream`]s.
//! The dataflow graph is thread-local; therefore, drivers should not be
//! multi-threaded and this module should never be used from an asynchronous
//! context.
use std::cell::RefCell;

use serde::Deserialize;

use crate::{
    dataflow::{
        stream::{ExtractStream, IngestStream, LoopStream, Stream, StreamId, StreamOrigin},
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
pub(crate) fn add_operator<F: OperatorRunner>(
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

pub(crate) fn add_operator_stream<D>(operator_id: OperatorId, write_stream: &Stream<D>)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut()
            .add_operator_stream(operator_id, write_stream);
    });
}

pub(crate) fn add_ingest_stream<D, F: StreamSetupHook>(
    ingest_stream: &IngestStream<D>,
    setup_hook: F,
) where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut().add_ingest_stream(ingest_stream, setup_hook);
    });
}

pub(crate) fn add_extract_stream<D, F: StreamSetupHook>(
    extract_stream: &ExtractStream<D>,
    setup_hook: F,
) where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut()
            .add_extract_stream(extract_stream, setup_hook);
    });
}

pub(crate) fn add_loop_stream<D>(loop_stream: &LoopStream<D>)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.with(|g| {
        g.borrow_mut().add_loop_stream(loop_stream);
    });
}

/// Adds an alias from from_id to to_id on the default graph.
pub(crate) fn add_stream_alias(from_id: StreamId, to_id: StreamId) -> Result<(), String> {
    DEFAULT_GRAPH.with(|g| g.borrow_mut().add_stream_alias(from_id, to_id))
}

#[allow(unused_variables)]
pub(crate) fn set_stream_name(stream_id: &StreamId, name: &str) {
    unimplemented!("TODO(peter): implement when refactoring the graph.")
}

#[allow(unused_variables)]
pub(crate) fn get_stream_name(stream_id: &StreamId) -> String {
    unimplemented!("TODO(peter): implement when refactoring the graph.")
}

#[allow(unused_variables)]
pub(crate) fn set_stream_origin(stream_id: &StreamId, origin: StreamOrigin) {
    unimplemented!("TODO(peter): implement when refactoring the graph.")
}

#[allow(unused_variables)]
pub(crate) fn get_stream_origin(stream_id: &StreamId) -> StreamOrigin {
    unimplemented!("TODO(peter): implement when refactoring the graph.")
}

pub(crate) fn clone() -> Graph {
    DEFAULT_GRAPH.with(|g| g.borrow().clone())
}

/// Updates the graph, and returns previous value
pub(crate) fn set(graph: Graph) -> Graph {
    DEFAULT_GRAPH.with(|g| g.replace(graph))
}
