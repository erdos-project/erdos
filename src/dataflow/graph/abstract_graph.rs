use std::{collections::HashMap, marker::PhantomData};

use serde::Deserialize;

use crate::{
    dataflow::{
        stream::{ExtractStream, IngestStream, Stream, StreamId, StreamOrigin},
        Data, LoopStream,
    },
    OperatorConfig, OperatorId,
};

use super::{OperatorRunner, StreamSetupHook};

struct AbstractStream<D: Data> {
    id: StreamId,
    name: String,
    phantom: PhantomData<D>,
}

impl<D: Data> AbstractStream<D> {
    fn new(id: StreamId, name: String) -> Self {
        Self {
            id,
            name,
            phantom: PhantomData,
        }
    }
}

trait AbstractStreamT {
    fn id(&self) -> StreamId;
    fn name(&self) -> String;
    fn set_name(&mut self, name: String);
}

impl<D: Data> AbstractStreamT for AbstractStream<D> {
    fn id(&self) -> StreamId {
        self.id
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }
}

struct AbstractOperator {
    id: OperatorId,
    /// Function that executes the operator.
    runner: Box<dyn OperatorRunner>,
    /// Operator configuration.
    config: OperatorConfig,
    /// Streams on which the operator reads.
    read_streams: Vec<StreamId>,
    /// Streams on which the operator writes.
    write_streams: Vec<StreamId>,
}

/// The abstract graph representation of an ERDOS program defined
/// in the driver.
/// The abstract graph is compiled and scheduled before an ERDOS
/// program is run.
pub struct AbstractGraph {
    /// Collection of operators.
    operators: HashMap<OperatorId, AbstractOperator>,
    /// Collection of all streams in the graph.
    streams: HashMap<StreamId, Box<dyn AbstractStreamT>>,
    /// Collection of ingest streams and the corresponding functions to execute for setup.
    ingest_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
    /// Collection of extract streams and the corresponding functions to execute for setup.
    extract_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
    /// Collection of loop streams and the streams to which they connect.
    loop_streams: HashMap<StreamId, Option<StreamId>>,
}

impl AbstractGraph {
    /// Adds an operator and its read and write streams to the graph.
    /// Write streams are automatically named based on the operator name.
    pub(crate) fn add_operator<F: OperatorRunner, T: Data, U: Data, V: Data, W: Data>(
        &mut self,
        config: OperatorConfig,
        runner: F,
        left_read_stream: Option<&Stream<T>>,
        right_read_stream: Option<&Stream<U>>,
        left_write_stream: Option<&Stream<V>>,
        right_write_stream: Option<&Stream<W>>,
    ) {
        let read_streams = match (left_read_stream, right_read_stream) {
            (Some(ls), Some(rs)) => vec![ls.id(), rs.id()],
            (Some(ls), None) => vec![ls.id()],
            (None, Some(rs)) => vec![rs.id()],
            (None, None) => vec![],
        };

        let write_streams = match (left_write_stream, right_write_stream) {
            (Some(ls), Some(rs)) => vec![ls.id(), rs.id()],
            (Some(ls), None) => vec![ls.id()],
            (None, Some(rs)) => vec![rs.id()],
            (None, None) => vec![],
        };

        // Add write streams to the graph.
        if let Some(ls) = left_write_stream {
            let stream_name = if write_streams.len() == 1 {
                format!("{}-stream", config.get_name())
            } else {
                format!("{}-left-stream", config.get_name())
            };
            let abstract_stream = AbstractStream::<V>::new(ls.id(), stream_name);
            self.streams.insert(ls.id(), Box::new(abstract_stream));
        }
        if let Some(rs) = right_write_stream {
            let stream_name = format!("{}-right-stream", config.get_name());
            let abstract_stream = AbstractStream::<V>::new(rs.id(), stream_name);
            self.streams.insert(rs.id(), Box::new(abstract_stream));
        }

        let operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: operator_id,
            runner: Box::new(runner),
            config: config,
            read_streams,
            write_streams,
        };
        self.operators.insert(operator_id, abstract_operator);
    }

    /// Adds an [`IngestStream`] to the graph.
    /// [`IngestStream`]s are automatically named based on the number of [`IngestStream`]s
    /// in the graph.
    pub(crate) fn add_ingest_stream<D>(&mut self, ingest_stream: &IngestStream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let name = format!("ingest-stream-{}", self.ingest_streams.len());
        let abstract_stream = AbstractStream::<D>::new(ingest_stream.id(), name);
        self.streams
            .insert(ingest_stream.id(), Box::new(abstract_stream));

        let setup_hook = ingest_stream.get_setup_hook();
        self.ingest_streams
            .insert(ingest_stream.id(), Box::new(setup_hook));
    }

    /// Adds an [`ExtractStream`] to the graph.
    /// [`ExtractStream`]s are automatically named based on the number of [`ExtractStream`]s
    /// in the graph.
    pub(crate) fn add_extract_stream<D, F: StreamSetupHook>(
        &mut self,
        extract_stream: &ExtractStream<D>,
        setup_hook: F,
    ) where
        for<'a> D: Data + Deserialize<'a>,
    {
        let setup_hook = extract_stream.get_setup_hook();
        self.extract_streams
            .insert(extract_stream.id(), Box::new(setup_hook));
    }

    /// Adds a [`LoopStream`] to the graph.
    pub(crate) fn add_loop_stream<D>(&mut self, loop_stream: &LoopStream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let name = format!("loop-stream-{}", self.loop_streams.len());
        let abstract_stream = AbstractStream::<D>::new(loop_stream.id(), name);
        self.streams
            .insert(loop_stream.id(), Box::new(abstract_stream));

        self.loop_streams.insert(loop_stream.id(), None);
    }

    /// Connects a [`LoopStream`] to another stream in order to close a loop.
    pub(crate) fn connect_loop<D>(&mut self, loop_stream: &LoopStream<D>, stream: &Stream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        if let Some(v) = self.loop_streams.get_mut(&loop_stream.id()) {
            *v = Some(stream.id());
        }
    }

    pub(crate) fn get_stream_name(&self, stream_id: &StreamId) -> String {
        self.streams.get(stream_id).unwrap().name()
    }

    pub(crate) fn set_stream_name(&mut self, stream_id: &StreamId, name: String) {
        self.streams.get_mut(stream_id).unwrap().set_name(name);
    }
}
