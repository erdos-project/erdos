use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use serde::Deserialize;

use crate::dataflow::{graph::InternalGraph, Data};

use super::{OperatorStream, StreamId};

/// Enables loops in the dataflow.
///
/// # Example
/// ```
/// # use erdos::dataflow::{Graph, stream::LoopStream, operator::{OperatorConfig}, operators::{FlatMapOperator}};
/// let graph = Graph::new();
/// let loop_stream: LoopStream<usize> = graph.add_loop_stream();
/// let output_stream = graph.connect_one_in_one_out(
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
    pub(crate) fn new(graph: Arc<Mutex<InternalGraph>>) -> Self {
        let id = StreamId::new_deterministic();

        tracing::debug!("Initializing a LoopStream with ID: {}", id,);

        Self {
            id,
            phantom: PhantomData,
            graph: Arc::clone(&graph),
        }
    }

    pub fn connect_loop(&mut self, stream: &OperatorStream<D>) {
        Arc::clone(&self.graph)
            .lock()
            .unwrap()
            .connect_loop(self, stream);

        tracing::debug!(
            "Connected LoopStream with ID: {} to stream named: {}",
            self.id,
            stream.name,
        );
    }

    pub(crate) fn get_id(&self) -> StreamId {
        self.id
    }
}
