//! A globally accessible dataflow graph.
//!
//! This module is used in the driver when connecting new operators,
//! or setting up [`IngestStream`]s, [`ExtractStream`]s, and [`LoopStream`]s.
//! It is also used to get and set [`Stream`] names.
use std::{ops::DerefMut, sync::Mutex};

use once_cell::sync::Lazy;
use serde::Deserialize;

use crate::{
    dataflow::{
        stream::{ExtractStream, IngestStream, LoopStream, OperatorStream, Stream, StreamId},
        Data,
    },
    OperatorConfig,
};

use super::{AbstractGraph, OperatorRunner};

static DEFAULT_GRAPH: Lazy<Mutex<AbstractGraph>> = Lazy::new(|| Mutex::new(AbstractGraph::new()));

/// Adds an operator to the default graph.
///
/// The operator is pinned on a given node.
pub(crate) fn add_operator<F, T, U, V, W>(
    config: OperatorConfig,
    runner: F,
    left_read_stream: Option<&dyn Stream<T>>,
    right_read_stream: Option<&dyn Stream<U>>,
    left_write_stream: Option<&OperatorStream<V>>,
    right_write_stream: Option<&OperatorStream<W>>,
) where
    F: OperatorRunner,
    for<'a> T: Data + Deserialize<'a>,
    for<'a> U: Data + Deserialize<'a>,
    for<'a> V: Data + Deserialize<'a>,
    for<'a> W: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.lock().unwrap().add_operator(
        config,
        runner,
        left_read_stream,
        right_read_stream,
        left_write_stream,
        right_write_stream,
    );
}

/// Adds an [`IngestStream`] to the default graph.
///
/// The stream can be used by the driver to insert data into the dataflow.
pub(crate) fn add_ingest_stream<D>(ingest_stream: &IngestStream<D>)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH
        .lock()
        .unwrap()
        .add_ingest_stream(ingest_stream);
}

/// Adds an [`ExtractStream`] to the default graph.
///
/// The stream can be used by the driver to read data from the dataflow.
pub(crate) fn add_extract_stream<D>(extract_stream: &ExtractStream<D>)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH
        .lock()
        .unwrap()
        .add_extract_stream(extract_stream);
}

/// Adds a [`LoopStream`] to the default graph.
///
/// The stream can be used by the driver to create cycles in the dataflow.
pub(crate) fn add_loop_stream<D>(loop_stream: &LoopStream<D>)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH.lock().unwrap().add_loop_stream(loop_stream);
}

pub(crate) fn connect_loop<D>(loop_stream: &LoopStream<D>, stream: &OperatorStream<D>)
where
    for<'a> D: Data + Deserialize<'a>,
{
    DEFAULT_GRAPH
        .lock()
        .unwrap()
        .connect_loop(loop_stream, stream);
}

pub(crate) fn set_stream_name(stream_id: &StreamId, name: &str) {
    DEFAULT_GRAPH
        .lock()
        .unwrap()
        .set_stream_name(stream_id, name.to_string());
}

pub(crate) fn get_stream_name(stream_id: &StreamId) -> String {
    DEFAULT_GRAPH.lock().unwrap().get_stream_name(stream_id)
}

pub(crate) fn resolve_stream_id(stream_id: &StreamId) -> Option<StreamId> {
    DEFAULT_GRAPH.lock().unwrap().resolve_stream_id(stream_id)
}

pub(crate) fn clone() -> AbstractGraph {
    DEFAULT_GRAPH.lock().unwrap().clone()
}

/// Updates the graph, and returns previous value
pub(crate) fn set(graph: AbstractGraph) -> AbstractGraph {
    std::mem::replace(DEFAULT_GRAPH.lock().unwrap().deref_mut(), graph)
}
