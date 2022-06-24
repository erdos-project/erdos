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
    name: Option<String>,
    phantom: PhantomData<D>,
    graph: Arc<Mutex<InternalGraph>>,
}

impl<D> LoopStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub(crate) fn new(graph: Arc<Mutex<InternalGraph>>) -> Self {
        let id = StreamId::new_deterministic();
        Self {
            id,
            name: None,
            phantom: PhantomData,
            graph: Arc::clone(&graph),
        }
    }

    pub fn connect_loop(&mut self, stream: &OperatorStream<D>) {
        self.name = Some(stream.clone().name());
        self.get_graph().lock().unwrap().connect_loop(self, stream);

        tracing::debug!(
            "Connected LoopStream with ID: {} to stream with name: {:?}",
            self.id,
            self.name
        );
    }
}

impl<D> Stream<D> for LoopStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn id(&self) -> StreamId {
        self.id
    }
    fn name(&self) -> String {
        match &self.name {
            Some(name) => name.to_string(),
            None => {
                tracing::error!("LoopStream with ID: {} does not have a name set", self.id);
                "LoopStream".to_string()
            }
        }
    }
    fn get_graph(&self) -> std::sync::Arc<std::sync::Mutex<InternalGraph>> {
        Arc::clone(&self.graph)
    }
}
