use std::collections::HashMap;

use serde::Deserialize;

use crate::{
    dataflow::{
        stream::{ExtractStream, IngestStream, Stream, StreamId},
        Data, LoopStream,
    },
    OperatorConfig, OperatorId,
};

use super::{
    job_graph::JobGraph, AbstractOperator, AbstractStream, AbstractStreamT, OperatorRunner,
    StreamSetupHook,
};

/// The abstract graph representation of an ERDOS program defined in the driver.
///
/// The abstract graph is compiled into a [`JobGraph`], which ERDOS schedules and executes.
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
    ) where
        F: OperatorRunner,
        for<'a> T: Data + Deserialize<'a>,
        for<'a> U: Data + Deserialize<'a>,
        for<'a> V: Data + Deserialize<'a>,
        for<'a> W: Data + Deserialize<'a>,
    {
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

    /// Compiles the abstract graph defined into a physical plan
    /// consisting of jobs and typed communication channels connecting jobs.
    /// The compilation step checks that all [`LoopStream`]s are connected,
    /// and arranges jobs and channels in a directed graph.
    pub(crate) fn compile(&mut self) -> JobGraph {
        // Check that all loops are closed.
        for (loop_stream_id, connected_stream_id) in self.loop_streams.iter() {
            if connected_stream_id.is_none() {
                panic!("LoopStream {} is not connected to another loop. Call `LoopStream::connect_loop` to fix.", loop_stream_id);
            }
        }

        // Get all streams except loop streams.
        let streams: Vec<_> = self
            .streams
            .iter()
            .filter(|(k, _)| !self.loop_streams.contains_key(k))
            .map(|(_k, v)| v.box_clone())
            .collect();

        let mut ingest_streams = HashMap::new();
        let ingest_stream_ids: Vec<_> = self.ingest_streams.keys().cloned().collect();
        for stream_id in ingest_stream_ids {
            // Remove and re-insert setup hook to satisfy static lifetimes.
            let setup_hook = self.ingest_streams.remove(&stream_id).unwrap();
            ingest_streams.insert(stream_id, setup_hook.box_clone());
            self.ingest_streams.insert(stream_id, setup_hook);
        }

        let mut extract_streams = HashMap::new();
        let extract_stream_ids: Vec<_> = self.extract_streams.keys().cloned().collect();
        for stream_id in extract_stream_ids {
            // Remove and re-insert setup hook to satisfy static lifetimes.
            let setup_hook = self.extract_streams.remove(&stream_id).unwrap();
            extract_streams.insert(stream_id, setup_hook.box_clone());
            self.extract_streams.insert(stream_id, setup_hook);
        }

        let operators: Vec<_> = self.operators.values().cloned().collect();

        JobGraph::new(operators, streams, ingest_streams, extract_streams)
    }
}
