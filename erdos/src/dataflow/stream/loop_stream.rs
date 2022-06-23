use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use serde::Deserialize;

use crate::dataflow::{graph::InternalGraph, Data};

use super::{OperatorStream, Stream, StreamId};

/// Enables loops in the dataflow.
///
/// # Example
/// ```
/// # use erdos::dataflow::{stream::LoopStream, operator::{OperatorConfig}, operators::{FlatMapOperator}};
/// let loop_stream = LoopStream::new();
/// let output_stream = erdos::connect_one_in_one_out(
///     || FlatMapOperator::new(|x: &usize| { std::iter::once(2 * x) }),
///     || {},
///     OperatorConfig::new().name("MapOperator"),
///     &loop_stream,
/// );
/// // Makes sending on output_stream equivalent to sending on loop_stream.
/// loop_stream.connect_loop(&output_stream);
/// ```
pub struct LoopStream<D: Data>
where
    for<'a> D: Data + Deserialize<'a>,
{
    id: StreamId,
    phantom: PhantomData<D>,
    graph: Arc<Mutex<InternalGraph>>,
}

impl<D> LoopStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub fn new(graph: Arc<Mutex<InternalGraph>>) -> Self {
        let id = StreamId::new_deterministic();
        let loop_stream = Self {
            id,
            phantom: PhantomData,
            graph: Arc::clone(&graph),
        };
        graph.lock().unwrap().add_loop_stream(&loop_stream);
        loop_stream
    }

    pub fn connect_loop(&self, stream: &OperatorStream<D>) {
        self.get_graph().lock().unwrap().connect_loop(self, stream);
    }
}

impl<D> Stream<D> for LoopStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn id(&self) -> StreamId {
        self.id
    }
    fn get_graph(&self) -> std::sync::Arc<std::sync::Mutex<InternalGraph>> {
        Arc::clone(&self.graph)
    }
}
