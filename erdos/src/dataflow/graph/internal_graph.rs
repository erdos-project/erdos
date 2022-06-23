use std::collections::HashMap;

use serde::Deserialize;

use std::sync::{Arc, Mutex};

use crate::{
    dataflow::{
        operator::{
            OneInOneOut, OneInTwoOut, ParallelOneInOneOut, ParallelOneInTwoOut, ParallelSink,
            ParallelTwoInOneOut, Sink, Source, TwoInOneOut,
        },
        stream::{ExtractStream, IngestStream, OperatorStream, Stream, StreamId},
        AppendableState, Data, LoopStream, State,
    },
    node::operator_executors::{
        OneInExecutor, OneInOneOutMessageProcessor, OneInTwoOutMessageProcessor, OperatorExecutorT,
        ParallelOneInOneOutMessageProcessor, ParallelOneInTwoOutMessageProcessor,
        ParallelSinkMessageProcessor, ParallelTwoInOneOutMessageProcessor, SinkMessageProcessor,
        SourceExecutor, TwoInExecutor, TwoInOneOutMessageProcessor,
    },
    scheduler::channel_manager::ChannelManager,
    OperatorConfig, OperatorId,
};

use super::{
    job_graph::JobGraph, AbstractOperator, AbstractStream, AbstractStreamT, OperatorRunner,
    StreamSetupHook,
};

/// The abstract graph representation of an ERDOS program defined in the driver.
///
/// The abstract graph is compiled into a [`JobGraph`], which ERDOS schedules and executes.
pub struct InternalGraph {
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

impl InternalGraph {
    pub fn new() -> Self {
        Self {
            operators: HashMap::new(),
            streams: HashMap::new(),
            ingest_streams: HashMap::new(),
            extract_streams: HashMap::new(),
            loop_streams: HashMap::new(),
        }
    }

    /// Adds an operator and its read and write streams to the graph.
    /// Write streams are automatically named based on the operator name.
    pub(crate) fn add_operator<F, T, U, V, W>(
        &mut self,
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
                format!("{}-write-stream", config.get_name())
            } else {
                format!("{}-write-left-stream", config.get_name())
            };
            let abstract_stream = AbstractStream::<V>::new(ls.id(), stream_name);
            self.streams.insert(ls.id(), Box::new(abstract_stream));
        }
        if let Some(rs) = right_write_stream {
            let stream_name = format!("{}-right-write-stream", config.get_name());
            let abstract_stream = AbstractStream::<W>::new(rs.id(), stream_name);
            self.streams.insert(rs.id(), Box::new(abstract_stream));
        }

        let operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: operator_id,
            runner: Box::new(runner),
            config,
            read_streams,
            write_streams,
        };
        self.operators.insert(operator_id, abstract_operator);
    }

    /// Adds an [`IngestStream`] to the graph.
    /// [`IngestStream`]s are automatically named based on the number of [`IngestStream`]s
    /// in the graph.
    pub(crate) fn add_ingest_stream<D>(
        &mut self,
        ingest_stream: &IngestStream<D>,
        setup_hook: impl StreamSetupHook,
    ) where
        for<'a> D: Data + Deserialize<'a>,
    {
        // Note: do not call IngestStream::name or IngestStream::set_name, as this will try to
        // acquire a lock to this graph, causing a deadlock.
        let name = format!("ingest-stream-{}", self.ingest_streams.len());
        let abstract_stream = AbstractStream::<D>::new(ingest_stream.id(), name);
        self.streams
            .insert(ingest_stream.id(), Box::new(abstract_stream));

        self.ingest_streams
            .insert(ingest_stream.id(), Box::new(setup_hook));
    }

    /// Adds an [`ExtractStream`] to the graph.
    /// [`ExtractStream`]s are automatically named based on the number of [`ExtractStream`]s
    /// in the graph.
    pub(crate) fn add_extract_stream<D>(
        &mut self,
        extract_stream: &ExtractStream<D>,
        setup_hook: impl StreamSetupHook,
    ) where
        for<'a> D: Data + Deserialize<'a>,
    {
        // Note: do not call IngestStream::name or IngestStream::set_name, as this will try to
        // acquire a lock to this graph, causing a deadlock.
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
    pub(crate) fn connect_loop<D>(
        &mut self,
        loop_stream: &LoopStream<D>,
        stream: &OperatorStream<D>,
    ) where
        for<'a> D: Data + Deserialize<'a>,
    {
        if let Some(v) = self.loop_streams.get_mut(&loop_stream.id()) {
            *v = Some(stream.id());
        }
    }

    /// Adds a [`Source`] operator, which has no read streams, but introduces data into the dataflow
    /// graph by interacting with external data sources (e.g., other systems, sensor data).
    pub(crate) fn connect_source<O, T>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        write_stream: &OperatorStream<T>,
    ) where
        O: 'static + Source<T>,
        T: Data + for<'a> Deserialize<'a>,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let write_stream_id = write_stream.id();
        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let write_stream = channel_manager.write_stream(write_stream_id).unwrap();

                let executor =
                    SourceExecutor::new(config_copy.clone(), operator_fn.clone(), write_stream);

                Box::new(executor)
            };

        self.add_operator::<_, (), (), T, ()>(
            config,
            op_runner,
            None,
            None,
            Some(write_stream),
            None,
        );
    }

    /// Adds a [`ParallelSink`] operator, which receives data on input read streams and directly
    /// interacts with external systems.
    pub(crate) fn connect_parallel_sink<O, S, T, U>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        read_stream: &dyn Stream<T>,
    ) where
        O: 'static + ParallelSink<S, T, U>,
        S: AppendableState<U>,
        T: Data + for<'a> Deserialize<'a>,
        U: 'static + Send + Sync,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let read_stream_id = read_stream.id();
        let stream_id = self.resolve_stream_id(&read_stream_id).unwrap();

        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let read_stream = channel_manager.take_read_stream(stream_id).unwrap();

                Box::new(OneInExecutor::new(
                    config_copy.clone(),
                    Box::new(ParallelSinkMessageProcessor::new(
                        config_copy.clone(),
                        operator_fn.clone(),
                        state_fn.clone(),
                    )),
                    read_stream,
                ))
            };

        self.add_operator::<_, T, (), (), ()>(
            config,
            op_runner,
            Some(read_stream),
            None,
            None,
            None,
        );
    }

    /// Adds a [`Sink`] operator, which receives data on input read streams and directly interacts
    /// with external systems.
    pub(crate) fn connect_sink<O, S, T>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        read_stream: &dyn Stream<T>,
    ) where
        O: 'static + Sink<S, T>,
        S: State,
        T: Data + for<'a> Deserialize<'a>,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let read_stream_id = read_stream.id();
        let stream_id = self.resolve_stream_id(&read_stream_id).unwrap();

        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let read_stream = channel_manager.take_read_stream(stream_id).unwrap();

                Box::new(OneInExecutor::new(
                    config_copy.clone(),
                    Box::new(SinkMessageProcessor::new(
                        config_copy.clone(),
                        operator_fn.clone(),
                        state_fn.clone(),
                    )),
                    read_stream,
                ))
            };

        self.add_operator::<_, T, (), (), ()>(
            config,
            op_runner,
            Some(read_stream),
            None,
            None,
            None,
        );
    }

    /// Adds a [`ParallelOneInOneOut`] operator that has one input read stream and one output
    /// write stream.
    pub(crate) fn connect_parallel_one_in_one_out<O, S, T, U, V>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        read_stream: &dyn Stream<T>,
        write_stream: &OperatorStream<U>,
    ) where
        O: 'static + ParallelOneInOneOut<S, T, U, V>,
        S: AppendableState<V>,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: 'static + Send + Sync,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let read_stream_id = read_stream.id();
        let write_stream_id = write_stream.id();
        let stream_id = self.resolve_stream_id(&read_stream_id).unwrap();

        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let read_stream = channel_manager.take_read_stream(stream_id).unwrap();
                let write_stream = channel_manager.write_stream(write_stream_id).unwrap();

                Box::new(OneInExecutor::new(
                    config_copy.clone(),
                    Box::new(ParallelOneInOneOutMessageProcessor::new(
                        config_copy.clone(),
                        operator_fn.clone(),
                        state_fn.clone(),
                        write_stream,
                    )),
                    read_stream,
                ))
            };

        self.add_operator::<_, T, (), U, ()>(
            config,
            op_runner,
            Some(read_stream),
            None,
            Some(write_stream),
            None,
        );
    }

    /// Adds a [`OneInOneOut`] operator that has one input read stream and one output write stream.
    pub(crate) fn connect_one_in_one_out<O, S, T, U>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        read_stream: &dyn Stream<T>,
        write_stream: &OperatorStream<U>,
    ) where
        O: 'static + OneInOneOut<S, T, U>,
        S: State,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let read_stream_id = read_stream.id();
        let write_stream_id = write_stream.id();
        let stream_id = self.resolve_stream_id(&read_stream_id).unwrap();

        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let read_stream = channel_manager.take_read_stream(stream_id).unwrap();
                let write_stream = channel_manager.write_stream(write_stream_id).unwrap();

                Box::new(OneInExecutor::new(
                    config_copy.clone(),
                    Box::new(OneInOneOutMessageProcessor::new(
                        config_copy.clone(),
                        operator_fn.clone(),
                        state_fn.clone(),
                        write_stream,
                    )),
                    read_stream,
                ))
            };

        self.add_operator::<_, T, (), U, ()>(
            config,
            op_runner,
            Some(read_stream),
            None,
            Some(write_stream),
            None,
        );
    }

    /// Adds a [`ParallelTwoInOneOut`] operator that has two input read streams and one output
    /// write stream.
    pub(crate) fn connect_parallel_two_in_one_out<O, S, T, U, V, W>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        left_read_stream: &dyn Stream<T>,
        right_read_stream: &dyn Stream<U>,
        write_stream: &OperatorStream<V>,
    ) where
        O: 'static + ParallelTwoInOneOut<S, T, U, V, W>,
        S: AppendableState<W>,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: Data + for<'a> Deserialize<'a>,
        W: 'static + Send + Sync,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let left_read_stream_id = left_read_stream.id();
        let right_read_stream_id = right_read_stream.id();
        let write_stream_id = write_stream.id();
        let left_stream_id = self.resolve_stream_id(&left_read_stream_id).unwrap();
        let right_stream_id = self.resolve_stream_id(&right_read_stream_id).unwrap();

        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let left_read_stream = channel_manager.take_read_stream(left_stream_id).unwrap();
                let right_read_stream = channel_manager.take_read_stream(right_stream_id).unwrap();
                let write_stream = channel_manager.write_stream(write_stream_id).unwrap();

                Box::new(TwoInExecutor::new(
                    config_copy.clone(),
                    Box::new(ParallelTwoInOneOutMessageProcessor::new(
                        config_copy.clone(),
                        operator_fn.clone(),
                        state_fn.clone(),
                        write_stream,
                    )),
                    left_read_stream,
                    right_read_stream,
                ))
            };

        self.add_operator::<_, T, U, V, ()>(
            config,
            op_runner,
            Some(left_read_stream),
            Some(right_read_stream),
            Some(write_stream),
            None,
        );
    }

    /// Adds a [`TwoInOneOut`] operator that has two input read streams and one output write stream.
    pub fn connect_two_in_one_out<O, S, T, U, V>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        left_read_stream: &dyn Stream<T>,
        right_read_stream: &dyn Stream<U>,
        write_stream: &OperatorStream<V>,
    ) where
        O: 'static + TwoInOneOut<S, T, U, V>,
        S: State,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: Data + for<'a> Deserialize<'a>,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let left_read_stream_id = left_read_stream.id();
        let right_read_stream_id = right_read_stream.id();
        let write_stream_id = write_stream.id();
        let left_stream_id = self.resolve_stream_id(&left_read_stream_id).unwrap();
        let right_stream_id = self.resolve_stream_id(&right_read_stream_id).unwrap();

        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let left_read_stream = channel_manager.take_read_stream(left_stream_id).unwrap();
                let right_read_stream = channel_manager.take_read_stream(right_stream_id).unwrap();
                let write_stream = channel_manager.write_stream(write_stream_id).unwrap();

                Box::new(TwoInExecutor::new(
                    config_copy.clone(),
                    Box::new(TwoInOneOutMessageProcessor::new(
                        config_copy.clone(),
                        operator_fn.clone(),
                        state_fn.clone(),
                        write_stream,
                    )),
                    left_read_stream,
                    right_read_stream,
                ))
            };

        self.add_operator::<_, T, U, V, ()>(
            config,
            op_runner,
            Some(left_read_stream),
            Some(right_read_stream),
            Some(write_stream),
            None,
        );
    }

    /// Adds a [`ParallelOneInTwoOut`] operator that has one input read stream and two output
    /// write streams.
    pub fn connect_parallel_one_in_two_out<O, S, T, U, V, W>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        read_stream: &dyn Stream<T>,
        left_write_stream: &OperatorStream<U>,
        right_write_stream: &OperatorStream<V>,
    ) where
        O: 'static + ParallelOneInTwoOut<S, T, U, V, W>,
        S: AppendableState<W>,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: Data + for<'a> Deserialize<'a>,
        W: 'static + Send + Sync,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let read_stream_id = read_stream.id();
        let left_write_stream_id = left_write_stream.id();
        let right_write_stream_id = right_write_stream.id();
        let stream_id = self.resolve_stream_id(&read_stream_id).unwrap();

        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let read_stream = channel_manager.take_read_stream(stream_id).unwrap();
                let left_write_stream = channel_manager.write_stream(left_write_stream_id).unwrap();
                let right_write_stream =
                    channel_manager.write_stream(right_write_stream_id).unwrap();

                Box::new(OneInExecutor::new(
                    config_copy.clone(),
                    Box::new(ParallelOneInTwoOutMessageProcessor::new(
                        config_copy.clone(),
                        operator_fn.clone(),
                        state_fn.clone(),
                        left_write_stream,
                        right_write_stream,
                    )),
                    read_stream,
                ))
            };

        self.add_operator::<_, T, (), U, V>(
            config,
            op_runner,
            Some(read_stream),
            None,
            Some(left_write_stream),
            Some(right_write_stream),
        );
    }

    /// Adds a [`OneInTwoOut`] operator that has one input read stream and two output write streams.
    pub fn connect_one_in_two_out<O, S, T, U, V>(
        &mut self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        mut config: OperatorConfig,
        read_stream: &dyn Stream<T>,
        left_write_stream: &OperatorStream<U>,
        right_write_stream: &OperatorStream<V>,
    ) where
        O: 'static + OneInTwoOut<S, T, U, V>,
        S: State,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: Data + for<'a> Deserialize<'a>,
    {
        config.id = OperatorId::new_deterministic();

        let config_copy = config.clone();
        let read_stream_id = read_stream.id();
        let left_write_stream_id = left_write_stream.id();
        let right_write_stream_id = right_write_stream.id();
        let stream_id = self.resolve_stream_id(&read_stream_id).unwrap();

        let op_runner =
            move |channel_manager: Arc<Mutex<ChannelManager>>| -> Box<dyn OperatorExecutorT> {
                let mut channel_manager = channel_manager.lock().unwrap();

                let read_stream = channel_manager.take_read_stream(stream_id).unwrap();
                let left_write_stream = channel_manager.write_stream(left_write_stream_id).unwrap();
                let right_write_stream =
                    channel_manager.write_stream(right_write_stream_id).unwrap();

                Box::new(OneInExecutor::new(
                    config_copy.clone(),
                    Box::new(OneInTwoOutMessageProcessor::new(
                        config_copy.clone(),
                        operator_fn.clone(),
                        state_fn.clone(),
                        left_write_stream,
                        right_write_stream,
                    )),
                    read_stream,
                ))
            };

        self.add_operator::<_, T, (), U, V>(
            config,
            op_runner,
            Some(read_stream),
            None,
            Some(left_write_stream),
            Some(right_write_stream),
        );
    }

    pub(crate) fn get_stream_name(&self, stream_id: &StreamId) -> String {
        self.streams.get(stream_id).unwrap().name()
    }

    pub(crate) fn set_stream_name(&mut self, stream_id: &StreamId, name: String) {
        self.streams.get_mut(stream_id).unwrap().set_name(name);
    }

    /// If `stream_id` corresponds to a [`LoopStream`], returns the [`StreamId`] of the
    /// [`Stream`] to which it is connected. Returns [`None`] if unconnected.
    /// Otherwise, returns `stream_id`.
    pub(crate) fn resolve_stream_id(&self, stream_id: &StreamId) -> Option<StreamId> {
        match self.loop_streams.get(stream_id) {
            Some(connected_stream_id) => *connected_stream_id,
            None => Some(*stream_id),
        }
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

        let mut operators: Vec<_> = self.operators.values().cloned().collect();

        // Replace loop stream IDs with connected stream IDs.
        for o in operators.iter_mut() {
            for i in 0..o.read_streams.len() {
                if self.loop_streams.contains_key(&o.read_streams[i]) {
                    let resolved_id = self.resolve_stream_id(&o.read_streams[i]).unwrap();
                    o.read_streams[i] = resolved_id;
                }
            }
        }

        JobGraph::new(operators, streams, ingest_streams, extract_streams)
    }

    // TODO: implement this using the Clone trait.
    pub(crate) fn clone(&mut self) -> Self {
        let streams: HashMap<_, _> = self
            .streams
            .iter()
            .map(|(&k, v)| (k, v.box_clone()))
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

        Self {
            operators: self.operators.clone(),
            streams,
            ingest_streams,
            extract_streams,
            loop_streams: self.loop_streams.clone(),
        }
    }
}
