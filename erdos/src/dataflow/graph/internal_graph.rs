use std::collections::{HashMap, HashSet};

use serde::Deserialize;

use std::sync::{Arc, Mutex};

use crate::{
    communication::data_plane::StreamManager,
    dataflow::{
        graph::{AbstractOperator, AbstractStream, AbstractStreamT, JobGraph},
        operator::{
            OneInOneOut, OneInTwoOut, ParallelOneInOneOut, ParallelOneInTwoOut, ParallelSink,
            ParallelTwoInOneOut, Sink, Source, TwoInOneOut,
        },
        stream::{EgressStream, IngressStream, OperatorStream, Stream, StreamId},
        AppendableState, Data, LoopStream, State,
    },
    node::operator_executors::{
        OneInExecutor, OneInOneOutMessageProcessor, OneInTwoOutMessageProcessor, OperatorExecutorT,
        ParallelOneInOneOutMessageProcessor, ParallelOneInTwoOutMessageProcessor,
        ParallelSinkMessageProcessor, ParallelTwoInOneOutMessageProcessor, SinkMessageProcessor,
        SourceExecutor, TwoInExecutor, TwoInOneOutMessageProcessor,
    },
    OperatorConfig, OperatorId,
};

use super::{AbstractOperatorType, GraphCompilationError, Job, JobRunner};

/// A trait that holds a partial function that sets up an `Operator` pending the resolution
/// of the IDs of its [`ReadStream`]s.
trait OperatorRunner:
    'static
    + Fn(
        Option<StreamId>, // ID of the Left ReadStream
        Option<StreamId>, // ID of the Right ReadStream
        &mut StreamManager,
    ) -> Box<dyn OperatorExecutorT>
    + Sync
    + Send
{
    /// Clone the function into a Boxed object.
    fn box_clone(&self) -> Box<dyn OperatorRunner>;

    /// Convert the underlying function into a [`JobRunner`] that sets up the Operator.
    /// 
    /// This method retrieves the actual [`ReadStream`] IDs of the Operator after the compilation
    /// step has resolved the IDs of the LoopStreams, and returns a function that sets up the
    /// Operator.
    fn to_job_runner(self, operator: &AbstractOperator) -> Result<Box<dyn JobRunner>, GraphCompilationError>;
}

impl<
        T: 'static
            + Fn(Option<StreamId>, Option<StreamId>, &mut StreamManager) -> Box<dyn OperatorExecutorT>
            + Sync
            + Send
            + Clone,
    > OperatorRunner for T
{
    fn box_clone(&self) -> Box<dyn OperatorRunner> {
        Box::new(self.clone())
    }

    fn to_job_runner(
        self,
        operator: &AbstractOperator,
    ) -> Result<Box<dyn JobRunner>, GraphCompilationError> {
        match operator.operator_type {
            AbstractOperatorType::Source => Ok(Box::new(
                move |stream_manager| -> Option<Box<dyn OperatorExecutorT>> {
                    let mut stream_manager = stream_manager.lock().unwrap();
                    let operator_executor = (self)(None, None, &mut stream_manager);
                    Some(operator_executor)
                },
            )),
            AbstractOperatorType::ParallelSink
            | AbstractOperatorType::Sink
            | AbstractOperatorType::ParallelOneInOneOut
            | AbstractOperatorType::OneInOneOut
            | AbstractOperatorType::ParallelOneInTwoOut
            | AbstractOperatorType::OneInTwoOut => {
                if operator.read_streams.len() < 1 {
                    return Err(GraphCompilationError(format!(
                        "Could not find the ReadStream ID for Operator {}.",
                        operator.id,
                    )));
                }
                let read_stream_id = operator.read_streams[0];
                Ok(Box::new(
                    move |stream_manager| -> Option<Box<dyn OperatorExecutorT>> {
                        let mut stream_manager = stream_manager.lock().unwrap();
                        let operator_executor =
                            (self)(Some(read_stream_id), None, &mut stream_manager);
                        Some(operator_executor)
                    },
                ))
            }
            AbstractOperatorType::ParallelTwoInOneOut | AbstractOperatorType::TwoInOneOut => {
                if operator.read_streams.len() < 2 {
                    return Err(GraphCompilationError(format!(
                        "Could not find the ReadStream IDs for Operator {}",
                        operator.id,
                    )));
                }
                let left_read_stream_id = operator.read_streams[0];
                let right_read_stream_id = operator.read_streams[1];
                Ok(Box::new(
                    move |stream_manager| -> Option<Box<dyn OperatorExecutorT>> {
                        let mut stream_manager = stream_manager.lock().unwrap();
                        let operator_executor = (self)(
                            Some(left_read_stream_id),
                            Some(right_read_stream_id),
                            &mut stream_manager,
                        );
                        Some(operator_executor)
                    },
                ))
            }
        }
    }
}

impl Clone for Box<dyn OperatorRunner> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}

/// A trait that holds a partial function that sets up an [`IngressStream`] or an [`EgressStream`]
/// and is merged into a [`JobRunner`] for the [`Driver`](crate::dataflow::graph::Job) upon
/// compilation.
trait DriverStreamSetupHook: 'static + Fn(&mut StreamManager) + Sync + Send {
    // Clone the function into a Boxed object.
    fn box_clone(&self) -> Box<dyn DriverStreamSetupHook>;
}

impl<T: 'static + Fn(&mut StreamManager) + Sync + Send + Clone> DriverStreamSetupHook for T {
    fn box_clone(&self) -> Box<dyn DriverStreamSetupHook> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn DriverStreamSetupHook> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}

/// The abstract graph representation of an ERDOS program defined in the driver.
///
/// The abstract graph is compiled into a JobGraph, which ERDOS schedules and executes.
/// TODO: Make this struct private.
#[derive(Default, Clone)]
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
    ingress_streams: HashMap<StreamId, Box<dyn DriverStreamSetupHook>>,
    /// Collection of egress streams and the corresponding functions to execute for setup.
    egress_streams: HashMap<StreamId, Box<dyn DriverStreamSetupHook>>,
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
        let id_copy = ingress_stream.id();

        let setup_hook = move |stream_manager: &mut StreamManager| match stream_manager
            .take_write_stream(id_copy)
        {
            Ok(write_stream) => {
                write_stream_option_copy
                    .lock()
                    .unwrap()
                    .replace(write_stream);
            }
            Err(err) => panic!("Unable to setup IngressStream {}: {}", id_copy, err),
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
        let id_copy = egress_stream.id();

        let setup_hook = move |stream_manager: &mut StreamManager| match stream_manager
            .take_read_stream(id_copy, Job::Driver)
        {
            Ok(read_stream) => {
                read_stream_option_copy.lock().unwrap().replace(read_stream);
            }
            Err(err) => panic!("Unable to setup EgressStream {}: {}", id_copy, err),
        };

        self.egress_streams
            .insert(egress_stream.id(), Box::new(setup_hook));
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
            move |_, _, stream_manager: &mut StreamManager| -> Box<dyn OperatorExecutorT> {
                let write_stream = stream_manager.take_write_stream(write_stream_id).unwrap();

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

        let op_runner = move |read_stream_id: Option<StreamId>,
                              _,
                              stream_manager: &mut StreamManager|
              -> Box<dyn OperatorExecutorT> {
            let read_stream = stream_manager
                .take_read_stream(read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();

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
            read_streams: vec![read_stream.id()],
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

        let op_runner = move |read_stream_id: Option<StreamId>,
                              _,
                              stream_manager: &mut StreamManager|
              -> Box<dyn OperatorExecutorT> {
            let read_stream = stream_manager
                .take_read_stream(read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();

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
            read_streams: vec![read_stream.id()],
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
        let write_stream_id = write_stream.id();

        let op_runner = move |read_stream_id: Option<StreamId>,
                              _,
                              stream_manager: &mut StreamManager|
              -> Box<dyn OperatorExecutorT> {
            let read_stream = stream_manager
                .take_read_stream(read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();
            let write_stream = stream_manager.take_write_stream(write_stream_id).unwrap();

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
            read_streams: vec![read_stream.id()],
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
        let write_stream_id = write_stream.id();

        let op_runner = move |read_stream_id: Option<StreamId>,
                              _,
                              stream_manager: &mut StreamManager|
              -> Box<dyn OperatorExecutorT> {
            let read_stream = stream_manager
                .take_read_stream(read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();
            let write_stream = stream_manager.take_write_stream(write_stream_id).unwrap();

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
            read_streams: vec![read_stream.id()],
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
        let write_stream_id = write_stream.id();

        let op_runner = move |left_read_stream_id: Option<StreamId>,
                              right_read_stream_id: Option<StreamId>,
                              stream_manager: &mut StreamManager|
              -> Box<dyn OperatorExecutorT> {
            let left_read_stream = stream_manager
                .take_read_stream(left_read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();
            let right_read_stream = stream_manager
                .take_read_stream(right_read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();
            let write_stream = stream_manager.take_write_stream(write_stream_id).unwrap();

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
            read_streams: vec![left_read_stream.id(), right_read_stream.id()],
            write_streams: vec![write_stream_id],
            operator_type: AbstractOperatorType::ParallelTwoInOneOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

    pub(crate) fn connect_two_in_one_out<O, S, T, U, V>(
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
        let write_stream_id = write_stream.id();

        let op_runner = move |left_read_stream_id: Option<StreamId>,
                              right_read_stream_id: Option<StreamId>,
                              stream_manager: &mut StreamManager|
              -> Box<dyn OperatorExecutorT> {
            let left_read_stream = stream_manager
                .take_read_stream(left_read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();
            let right_read_stream = stream_manager
                .take_read_stream(right_read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();
            let write_stream = stream_manager.take_write_stream(write_stream_id).unwrap();

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
            read_streams: vec![left_read_stream.id(), right_read_stream.id()],
            write_streams: vec![write_stream_id],
            operator_type: AbstractOperatorType::TwoInOneOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

    pub(crate) fn connect_parallel_one_in_two_out<O, S, T, U, V, W>(
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
        let left_write_stream_id = left_write_stream.id();
        let right_write_stream_id = right_write_stream.id();

        let op_runner = move |read_stream_id: Option<StreamId>,
                              _,
                              stream_manager: &mut StreamManager|
              -> Box<dyn OperatorExecutorT> {
            let read_stream = stream_manager
                .take_read_stream(read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();
            let left_write_stream = stream_manager
                .take_write_stream(left_write_stream_id)
                .unwrap();
            let right_write_stream = stream_manager
                .take_write_stream(right_write_stream_id)
                .unwrap();

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
            read_streams: vec![read_stream.id()],
            write_streams: vec![left_write_stream_id, right_write_stream_id],
            operator_type: AbstractOperatorType::ParallelOneInTwoOut,
        };
        self.operators
            .insert(abstract_operator_id, abstract_operator);
        self.operator_runners
            .insert(abstract_operator_id, Box::new(op_runner));
    }

    pub(crate) fn connect_one_in_two_out<O, S, T, U, V>(
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
        let left_write_stream_id = left_write_stream.id();
        let right_write_stream_id = right_write_stream.id();

        let op_runner = move |read_stream_id: Option<StreamId>,
                              _,
                              stream_manager: &mut StreamManager|
              -> Box<dyn OperatorExecutorT> {
            let read_stream = stream_manager
                .take_read_stream(read_stream_id.unwrap(), Job::Operator(config_copy.id))
                .unwrap();
            let left_write_stream = stream_manager
                .take_write_stream(left_write_stream_id)
                .unwrap();
            let right_write_stream = stream_manager
                .take_write_stream(right_write_stream_id)
                .unwrap();

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
            read_streams: vec![read_stream.id()],
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
    pub(crate) fn compile(&mut self) -> Result<JobGraph, GraphCompilationError> {
        // Ensure that all loop streams are connected.
        for (loop_stream_id, connected_stream_id) in self.loop_streams.iter() {
            if connected_stream_id.is_none() {
                return Err(GraphCompilationError(format!(
                    "LoopStream {} is not connected to another loop. \
                        Call `LoopStream::connect_loop` to fix.",
                    loop_stream_id
                )));
            }
        }

        // Maintain the executors for each compiled Job.
        let mut job_runners = HashMap::with_capacity(self.operators.len() + 1);

        // Merge all the SetupHooks for the streams of the Driver job.
        let mut driver_setup_hooks = Vec::new();

        // Move the IngressStreams to the JobGraph while marking its source as the Driver,
        // and adding the setup hook to the set of driver setup hooks.
        let mut ingress_streams = HashSet::with_capacity(self.ingress_streams.len());
        for (ingress_stream_id, setup_hook) in self.ingress_streams.drain() {
            let ingress_stream = self.streams.get_mut(&ingress_stream_id).ok_or_else(|| {
                GraphCompilationError(format!(
                    "Unable to retrieve IngressStream with ID: {}",
                    ingress_stream_id
                ))
            })?;
            ingress_stream.register_source(Job::Driver);
            ingress_streams.insert(ingress_stream_id);
            driver_setup_hooks.push(setup_hook);
        }

        // Move the EgressStreams to the JobGraph while adding the Driver as a destination.
        let mut egress_streams = HashSet::with_capacity(self.egress_streams.len());
        for (egress_stream_id, setup_hook) in self.egress_streams.drain() {
            let egress_stream = self.streams.get_mut(&egress_stream_id).ok_or_else(|| {
                GraphCompilationError(format!(
                    "Unable to retrieve EgressStream with ID: {}",
                    egress_stream_id
                ))
            })?;
            egress_stream.add_destination(Job::Driver);
            egress_streams.insert(egress_stream_id);
            driver_setup_hooks.push(setup_hook);
        }

        // Merge the setup hooks of the Driver streams into a single JobRunner.
        if !driver_setup_hooks.is_empty() {
            let driver_runner: Box<dyn JobRunner> = Box::new(
                move |stream_manager: Arc<Mutex<StreamManager>>| -> Option<Box<dyn OperatorExecutorT>> {
                    let mut stream_manager = stream_manager.lock().unwrap();
                    for driver_setup_hook in driver_setup_hooks.clone() {
                        (driver_setup_hook)(&mut stream_manager);
                    }
                    None
                },
            );
            job_runners.insert(Job::Driver, driver_runner);
        }

        // Move all the non-loop Streams into the JobGraph.
        let mut streams: HashMap<_, _> = self.streams.drain().collect();
        streams.retain(|stream_id, _| !self.loop_streams.contains_key(stream_id));

        // Move AbstractOperators to the JobGraph while resolving LoopStream IDs
        // and setting the Sources and destinations of the streams.
        let mut operators: HashMap<_, _> = self
            .operators
            .drain()
            .map(|(operator_id, abstract_operator)| (Job::Operator(operator_id), abstract_operator))
            .collect();
        for (operator_job, abstract_operator) in operators.iter_mut() {
            for index in 0..abstract_operator.read_streams.len() {
                // If any ReadStream was connected to a LoopStream, resolve the ID.
                let stream_id = &abstract_operator.read_streams[index];
                let resolved_read_stream_id =
                    self.resolve_stream_id(stream_id).ok_or_else(|| {
                        GraphCompilationError(format!(
                            "Could not resolve the StreamID: {} for Operator {}",
                            stream_id, abstract_operator.id,
                        ))
                    })?;

                // Register the job as a destination with the ReadStream.
                let read_stream = streams.get_mut(&resolved_read_stream_id).ok_or_else(|| {
                    GraphCompilationError(format!(
                        "Could not find the ReadStream with ID {} for Operator {}",
                        resolved_read_stream_id, abstract_operator.id,
                    ))
                })?;
                read_stream.add_destination(operator_job.clone());

                // Save the resolved StreamID.
                abstract_operator.read_streams[index] = resolved_read_stream_id;
            }

            for write_stream_id in abstract_operator.write_streams.iter() {
                // Register the job as a source with the WriteStream.
                let write_stream = streams.get_mut(write_stream_id).ok_or_else(|| {
                    GraphCompilationError(format!(
                        "Could not find the WriteStream with ID {} for Operator {}",
                        write_stream_id, abstract_operator.id,
                    ))
                })?;
                write_stream.register_source(operator_job.clone());
            }

            // Create a JobRunner for the operator now that the IDs are resolved.
            let operator_runner = self
                .operator_runners
                .remove(&abstract_operator.id)
                .ok_or_else(|| {
                    GraphCompilationError(format!(
                        "Could not find an OperatorRunner for Operator {}",
                        abstract_operator.id
                    ))
                })?;
            job_runners.insert(
                operator_job.clone(),
                operator_runner.to_job_runner(&abstract_operator)?,
            );
        }

        // Ensure that all streams have Sources.
        for stream in streams.values() {
            if stream.source().is_none() {
                return Err(GraphCompilationError(format!(
                    "The Stream {} was not mapped to a Source.",
                    stream.id()
                )));
            }
        }

        Ok(JobGraph::new(
            self.name.clone(),
            operators,
            job_runners,
            streams,
            ingress_streams,
            egress_streams,
        ))
    }
}
