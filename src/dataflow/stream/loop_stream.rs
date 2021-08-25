use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use serde::Deserialize;

use crate::dataflow::{graph::default_graph, Data};

use super::{Stream, StreamId};

/// Enables loops in the dataflow.
///
/// # Example
/// ```ignore
/// let loop_stream = LoopStream::new();
/// let output_stream = erdos::connect_1_write!(MyOperator, OperatorConfig::new(), loop_stream);
/// // Makes sending on output_stream equivalent to sending on loop_stream.
/// loop_stream.set(&output_stream);
/// ```
pub struct LoopStream<D: Data>
where
    for<'a> D: Data + Deserialize<'a>,
{
    id: StreamId,
    pub(crate) name: Arc<Mutex<String>>,
    phantom: PhantomData<D>,
}

impl<D> LoopStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub fn new() -> Self {
        let id = StreamId::new_deterministic();
        LoopStream::new_internal(id, id.to_string())
    }

    pub fn new_with_name(name: &str) -> Self {
        LoopStream::new_internal(StreamId::new_deterministic(), name.to_string())
    }

    fn new_internal(id: StreamId, name: String) -> Self {
        let loop_stream = Self {
            id,
            name: Arc::new(Mutex::new(name)),
            phantom: PhantomData,
        };
        default_graph::add_loop_stream(&loop_stream);
        loop_stream
    }

    pub fn id(&self) -> StreamId {
        self.id
    }

    pub fn name(&self) -> String {
        self.name.lock().unwrap().clone()
    }

    pub fn set(&self, stream: &Stream<D>) {
        default_graph::add_stream_alias(self.id, stream.id()).unwrap();
    }
}
