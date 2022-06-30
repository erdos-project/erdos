use std::{collections::HashMap, hash::Hash};

use serde::Deserialize;

use std::sync::{Arc, Mutex};

use crate::{
    communication::data_plane::StreamManager,
    dataflow::{
        graph::{AbstractOperator, AbstractStream, AbstractStreamT, JobGraph, StreamSetupHook},
        operator::{
            OneInOneOut, OneInTwoOut, ParallelOneInOneOut, ParallelOneInTwoOut, ParallelSink,
            ParallelTwoInOneOut, Sink, Source, TwoInOneOut,
        },
        stream::{EgressStream, IngressStream, OperatorStream, Stream, StreamId},
        AppendableState, Data, LoopStream, ReadStream, State, WriteStream,
    },
    node::operator_executors::{
        OneInExecutor, OneInOneOutMessageProcessor, OneInTwoOutMessageProcessor, OperatorExecutorT,
        ParallelOneInOneOutMessageProcessor, ParallelOneInTwoOutMessageProcessor,
        ParallelSinkMessageProcessor, ParallelTwoInOneOutMessageProcessor, SinkMessageProcessor,
        SourceExecutor, TwoInExecutor, TwoInOneOutMessageProcessor,
    },
    OperatorConfig, OperatorId,
};

use super::{AbstractOperatorType, Job, OperatorRunner};

/// The abstract graph representation of an ERDOS program defined in the driver.
///
/// The abstract graph is compiled into a [`JobGraph`], which ERDOS schedules and executes.
#[derive(Default)]
pub struct InternalGraph {
    /// The name of the Graph.
    name: String,
    /// Collection of operators.
    operators: HashMap<OperatorId, AbstractOperator>,
    /// A mapping from the OperatorId to the OperatorRunner.
    operator_runners: HashMap<OperatorId, Box<dyn OperatorRunner>>,
    /// Collection of all streams in the graph.
    streams: HashMap<StreamId, Box<dyn AbstractStreamT>>,
    /// Collection of ingress streams and the corresponding functions to execute for setup.
    ingress_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
    /// Collection of egress streams and the corresponding functions to execute for setup.
    egress_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
    /// Collection of loop streams and the streams to which they connect.
    loop_streams: HashMap<StreamId, Option<StreamId>>,
}

impl InternalGraph {
    pub fn new(name: String) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }

    pub(crate) fn add_ingress_stream<D>(&mut self, ingress_stream: &IngressStream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        // A hook to initialize the ingress stream's connections to downstream operators.
        let write_stream_option_copy = ingress_stream.get_write_stream();
        let name_copy = ingress_stream.name();
        let id_copy = ingress_stream.id();
        let setup_hook = move |stream_manager: &mut StreamManager| match stream_manager
            .get_send_endpoints(id_copy)
        {
            Ok(send_endpoints) => {
                let write_stream = WriteStream::new(id_copy, &name_copy, send_endpoints);
                write_stream_option_copy
                    .lock()
                    .unwrap()
                    .replace(write_stream);
            }
            Err(msg) => panic!("Unable to set up IngressStream {}: {}", id_copy, msg),
        };

        self.streams.insert(
            ingress_stream.id(),
            Box::new(AbstractStream::from(ingress_stream)),
        );

        self.ingress_streams
            .insert(ingress_stream.id(), Box::new(setup_hook));
    }

    pub(crate) fn add_egress_stream<D>(&mut self, egress_stream: &EgressStream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let read_stream_option_copy = egress_stream.get_read_stream();
        let name_copy = egress_stream.name();
        let id_copy = egress_stream.id();

        let hook = move |stream_manager: &mut StreamManager| match stream_manager
            .take_recv_endpoint(id_copy)
        {
            Ok(recv_endpoint) => {
                let read_stream = ReadStream::new(id_copy, &name_copy, recv_endpoint);
                read_stream_option_copy.lock().unwrap().replace(read_stream);
            }
            Err(msg) => panic!("Unable to set up EgressStream {}: {}", id_copy, msg),
        };

        self.egress_streams
            .insert(egress_stream.id(), Box::new(hook));
    }

    pub(crate) fn add_loop_stream<D>(&mut self, loop_stream: &LoopStream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        self.streams.insert(
            loop_stream.id(),
            Box::new(AbstractStream::<D>::new(
                loop_stream.id(),
                format!("LoopStream-{}", loop_stream.id()),
            )),
        );

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let write_stream = stream_manager.write_stream(write_stream_id).unwrap();

                let executor =
                    SourceExecutor::new(config_copy.clone(), operator_fn.clone(), write_stream);

                Box::new(executor)
            };

        self.streams.insert(
            write_stream_id,
            Box::new(AbstractStream::from(write_stream)),
        );

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: abstract_operator_id,
            config,
            read_streams: vec![],
            write_streams: vec![write_stream_id],
            operator_type: AbstractOperatorType::Source,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let read_stream = stream_manager.take_read_stream(stream_id).unwrap();

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

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: abstract_operator_id,
            config,
            read_streams: vec![read_stream_id],
            write_streams: vec![],
            operator_type: AbstractOperatorType::ParallelSink,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let read_stream = stream_manager.take_read_stream(stream_id).unwrap();

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

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: abstract_operator_id,
            config,
            read_streams: vec![read_stream_id],
            write_streams: vec![],
            operator_type: AbstractOperatorType::Sink,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let read_stream = stream_manager.take_read_stream(stream_id).unwrap();
                let write_stream = stream_manager.write_stream(write_stream_id).unwrap();

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

        self.streams.insert(
            write_stream_id,
            Box::new(AbstractStream::from(write_stream)),
        );

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: abstract_operator_id,
            config,
            read_streams: vec![read_stream_id],
            write_streams: vec![write_stream_id],
            operator_type: AbstractOperatorType::ParallelOneInOneOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let read_stream = stream_manager.take_read_stream(stream_id).unwrap();
                let write_stream = stream_manager.write_stream(write_stream_id).unwrap();

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

        self.streams.insert(
            write_stream_id,
            Box::new(AbstractStream::from(write_stream)),
        );

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: abstract_operator_id,
            config,
            read_streams: vec![read_stream_id],
            write_streams: vec![write_stream_id],
            operator_type: AbstractOperatorType::OneInOneOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let left_read_stream = stream_manager.take_read_stream(left_stream_id).unwrap();
                let right_read_stream = stream_manager.take_read_stream(right_stream_id).unwrap();
                let write_stream = stream_manager.write_stream(write_stream_id).unwrap();

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

        self.streams.insert(
            write_stream_id,
            Box::new(AbstractStream::from(write_stream)),
        );

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: abstract_operator_id,
            config,
            read_streams: vec![left_read_stream_id, right_read_stream_id],
            write_streams: vec![write_stream_id],
            operator_type: AbstractOperatorType::ParallelTwoInOneOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let left_read_stream = stream_manager.take_read_stream(left_stream_id).unwrap();
                let right_read_stream = stream_manager.take_read_stream(right_stream_id).unwrap();
                let write_stream = stream_manager.write_stream(write_stream_id).unwrap();

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

        self.streams.insert(
            write_stream_id,
            Box::new(AbstractStream::from(write_stream)),
        );

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: abstract_operator_id,
            config,
            read_streams: vec![left_read_stream_id, right_read_stream_id],
            write_streams: vec![write_stream_id],
            operator_type: AbstractOperatorType::TwoInOneOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let read_stream = stream_manager.take_read_stream(stream_id).unwrap();
                let left_write_stream = stream_manager.write_stream(left_write_stream_id).unwrap();
                let right_write_stream =
                    stream_manager.write_stream(right_write_stream_id).unwrap();

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

        self.streams.insert(
            left_write_stream_id,
            Box::new(AbstractStream::from(left_write_stream)),
        );
        self.streams.insert(
            right_write_stream_id,
            Box::new(AbstractStream::from(right_write_stream)),
        );

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: abstract_operator_id,
            config,
            read_streams: vec![read_stream_id],
            write_streams: vec![left_write_stream_id, right_write_stream_id],
            operator_type: AbstractOperatorType::ParallelOneInTwoOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

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
            move |stream_manager: Arc<Mutex<StreamManager>>| -> Box<dyn OperatorExecutorT> {
                let mut stream_manager = stream_manager.lock().unwrap();

                let read_stream = stream_manager.take_read_stream(stream_id).unwrap();
                let left_write_stream = stream_manager.write_stream(left_write_stream_id).unwrap();
                let right_write_stream =
                    stream_manager.write_stream(right_write_stream_id).unwrap();

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

        self.streams.insert(
            left_write_stream_id,
            Box::new(AbstractStream::from(left_write_stream)),
        );
        self.streams.insert(
            right_write_stream_id,
            Box::new(AbstractStream::from(right_write_stream)),
        );

        let abstract_operator_id = config.id;
        let abstract_operator = AbstractOperator {
            id: config.id,
            config,
            read_streams: vec![read_stream_id],
            write_streams: vec![left_write_stream_id, right_write_stream_id],
            operator_type: AbstractOperatorType::OneInTwoOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
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

        let mut ingress_streams = HashMap::new();
        let ingress_stream_ids: Vec<_> = self.ingress_streams.keys().cloned().collect();
        for stream_id in ingress_stream_ids {
            // Remove and re-insert setup hook to satisfy static lifetimes.
            let setup_hook = self.ingress_streams.remove(&stream_id).unwrap();
            ingress_streams.insert(stream_id, setup_hook.box_clone());
            self.ingress_streams.insert(stream_id, setup_hook);
        }

        let mut egress_streams = HashMap::new();
        let egress_stream_ids: Vec<_> = self.egress_streams.keys().cloned().collect();
        for stream_id in egress_stream_ids {
            // Remove and re-insert setup hook to satisfy static lifetimes.
            let setup_hook = self.egress_streams.remove(&stream_id).unwrap();
            egress_streams.insert(stream_id, setup_hook.box_clone());
            self.egress_streams.insert(stream_id, setup_hook);
        }

        let mut operators: HashMap<_, _> = self
            .operators
            .iter()
            .map(|(operator_id, operator)| (Job::Operator(*operator_id), operator.clone()))
            .collect();

        // Replace loop stream IDs with connected stream IDs.
        for o in operators.values_mut() {
            for i in 0..o.read_streams.len() {
                if self.loop_streams.contains_key(&o.read_streams[i]) {
                    let resolved_id = self.resolve_stream_id(&o.read_streams[i]).unwrap();
                    o.read_streams[i] = resolved_id;
                }
            }
        }

        JobGraph::new(
            self.name.clone(),
            operators,
            self.operator_runners.clone(),
            streams,
            ingress_streams,
            egress_streams,
        )
    }
}
