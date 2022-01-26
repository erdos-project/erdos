use std::marker::PhantomData;

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
    phantom: PhantomData<D>,
}

impl<D> LoopStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub fn new() -> Self {
        let id = StreamId::new_deterministic();
        let loop_stream = Self {
            id,
            phantom: PhantomData,
        };
        default_graph::add_loop_stream(&loop_stream);
        loop_stream
    }

    pub fn id(&self) -> StreamId {
        self.id
    }

    pub fn connect_loop(&self, stream: &Stream<D>) {
        default_graph::connect_loop(self, stream);
    }
}
