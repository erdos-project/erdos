use std::sync::{Arc, Mutex};

use serde::Deserialize;

use crate::{
    dataflow::{
        graph::{InternalGraph, JobGraph},
        operator::{
            OneInOneOut, OneInTwoOut, ParallelOneInOneOut, ParallelOneInTwoOut, ParallelSink,
            ParallelTwoInOneOut, Sink, Source, TwoInOneOut,
        },
        stream::{IngressStream, OperatorStream},
        AppendableState, Data, LoopStream, State, Stream,
    },
    OperatorConfig,
};

use super::GraphCompilationError;

/// A user defined dataflow graph representation.
///
/// When adding various [`stream`](crate::dataflow::stream)s and
/// [`operators`](crate::dataflow::operators),
/// a graph is built up internally which manages all the connections between them.
/// This graph is compiled at runtime and can no longer be modified.
///
/// # Example
/// The below example shows how to create a [`Graph`] and run it.
/// ```no_run
/// # use erdos::dataflow::{
/// #    stream::{IngressStream, EgressStream, Stream},
/// #    operators::FlatMapOperator,
/// #    OperatorConfig, Message, Timestamp,
/// #    Graph,
/// # };
/// # use erdos::*;
/// #
/// // Create the Graph
/// let graph = Graph::new("GraphExample");
///
/// // Add streams and operators here
/// let mut ingress_stream: IngressStream<usize> = graph.add_ingress("ExampleIngressStream");
///
/// // Send data on the ingress stream
/// let msg = Message::new_message(Timestamp::Top, usize::MIN);
/// ingress_stream.send(msg).unwrap();
/// ```
///
/// See [`IngressStream`], [`EgressStream`](crate::dataflow::stream), [`LoopStream`], and the
/// [`operators`](crate::dataflow::operators) for more examples.
pub struct Graph {
    internal_graph: Arc<Mutex<InternalGraph>>,
}

#[allow(dead_code)]
impl Graph {
    /// Creates a new `Graph` with the given `name` to which streams and operators can be added.
    pub fn new(name: &str) -> Self {
        Self {
            internal_graph: Arc::new(Mutex::new(InternalGraph::new(name.to_string()))),
        }
    }

    /// Adds an [`IngressStream`] with the given `name` to the `Graph`.
    pub fn add_ingress<D>(&self, name: &str) -> IngressStream<D>
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let ingress_stream = IngressStream::new(name, Arc::clone(&self.internal_graph));
        self.internal_graph
            .lock()
            .unwrap()
            .add_ingress_stream(&ingress_stream);

        ingress_stream
    }

    /// Adds a [`LoopStream`] to the `Graph`.
    pub fn add_loop_stream<D>(&self) -> LoopStream<D>
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let loop_stream = LoopStream::new(Arc::clone(&self.internal_graph));
        self.internal_graph
            .lock()
            .unwrap()
            .add_loop_stream(&loop_stream);

        loop_stream
    }

    /// Adds a [`Source`] operator, which has no read streams, but introduces data into the dataflow
    /// graph by interacting with external data sources (e.g., other systems, sensor data).
    pub fn connect_source<O, T>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        config: OperatorConfig,
    ) -> OperatorStream<T>
    where
        O: 'static + Source<T>,
        T: Data + for<'a> Deserialize<'a>,
    {
        let write_stream = OperatorStream::new(
            &format!("{}-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        self.internal_graph
            .lock()
            .unwrap()
            .connect_source(operator_fn, config, &write_stream);

        write_stream
    }

    /// Adds a [`ParallelSink`] operator, which receives data on input read streams and directly
    /// interacts with external systems.
    pub fn connect_parallel_sink<O, S, T, U>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        config: OperatorConfig,
        read_stream: &dyn Stream<T>,
    ) where
        O: 'static + ParallelSink<S, T, U>,
        S: AppendableState<U>,
        T: Data + for<'a> Deserialize<'a>,
        U: 'static + Send + Sync,
    {
        self.internal_graph.lock().unwrap().connect_parallel_sink(
            operator_fn,
            state_fn,
            config,
            read_stream,
        )
    }

    /// Adds a [`Sink`] operator, which receives data on input read streams and directly interacts
    /// with external systems.
    pub fn connect_sink<O, S, T>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        config: OperatorConfig,
        read_stream: &dyn Stream<T>,
    ) where
        O: 'static + Sink<S, T>,
        S: State,
        T: Data + for<'a> Deserialize<'a>,
    {
        self.internal_graph
            .lock()
            .unwrap()
            .connect_sink(operator_fn, state_fn, config, read_stream)
    }

    /// Adds a [`ParallelOneInOneOut`] operator that has one input read stream and one output
    /// write stream.
    pub fn connect_parallel_one_in_one_out<O, S, T, U, V>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        config: OperatorConfig,
        read_stream: &dyn Stream<T>,
    ) -> OperatorStream<U>
    where
        O: 'static + ParallelOneInOneOut<S, T, U, V>,
        S: AppendableState<V>,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: 'static + Send + Sync,
    {
        let write_stream = OperatorStream::new(
            &format!("{}-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        self.internal_graph
            .lock()
            .unwrap()
            .connect_parallel_one_in_one_out(
                operator_fn,
                state_fn,
                config,
                read_stream,
                &write_stream,
            );

        write_stream
    }

    /// Adds a [`OneInOneOut`] operator that has one input read stream and one output write stream.
    pub fn connect_one_in_one_out<O, S, T, U>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        config: OperatorConfig,
        read_stream: &dyn Stream<T>,
    ) -> OperatorStream<U>
    where
        O: 'static + OneInOneOut<S, T, U>,
        S: State,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
    {
        let write_stream = OperatorStream::new(
            &format!("{}-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        self.internal_graph.lock().unwrap().connect_one_in_one_out(
            operator_fn,
            state_fn,
            config,
            read_stream,
            &write_stream,
        );

        write_stream
    }

    /// Adds a [`ParallelTwoInOneOut`] operator that has two input read streams and one output
    /// write stream.
    pub(crate) fn connect_parallel_two_in_one_out<O, S, T, U, V, W>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        config: OperatorConfig,
        left_read_stream: &dyn Stream<T>,
        right_read_stream: &dyn Stream<U>,
    ) -> OperatorStream<V>
    where
        O: 'static + ParallelTwoInOneOut<S, T, U, V, W>,
        S: AppendableState<W>,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: Data + for<'a> Deserialize<'a>,
        W: 'static + Send + Sync,
    {
        let write_stream = OperatorStream::new(
            &format!("{}-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        self.internal_graph
            .lock()
            .unwrap()
            .connect_parallel_two_in_one_out(
                operator_fn,
                state_fn,
                config,
                left_read_stream,
                right_read_stream,
                &write_stream,
            );

        write_stream
    }

    /// Adds a [`TwoInOneOut`] operator that has two input read streams and one output write stream.
    pub fn connect_two_in_one_out<O, S, T, U, V>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        config: OperatorConfig,
        left_read_stream: &dyn Stream<T>,
        right_read_stream: &dyn Stream<U>,
    ) -> OperatorStream<V>
    where
        O: 'static + TwoInOneOut<S, T, U, V>,
        S: State,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: Data + for<'a> Deserialize<'a>,
    {
        let write_stream = OperatorStream::new(
            &format!("{}-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        self.internal_graph.lock().unwrap().connect_two_in_one_out(
            operator_fn,
            state_fn,
            config,
            left_read_stream,
            right_read_stream,
            &write_stream,
        );

        write_stream
    }

    /// Adds a [`ParallelOneInTwoOut`] operator that has one input read stream and two output
    /// write streams.
    pub fn connect_parallel_one_in_two_out<O, S, T, U, V, W>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        config: OperatorConfig,
        read_stream: &dyn Stream<T>,
    ) -> (OperatorStream<U>, OperatorStream<V>)
    where
        O: 'static + ParallelOneInTwoOut<S, T, U, V, W>,
        S: AppendableState<W>,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: Data + for<'a> Deserialize<'a>,
        W: 'static + Send + Sync,
    {
        let left_write_stream = OperatorStream::new(
            &format!("{}-left-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        let right_write_stream = OperatorStream::new(
            &format!("{}-right-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        self.internal_graph
            .lock()
            .unwrap()
            .connect_parallel_one_in_two_out(
                operator_fn,
                state_fn,
                config,
                read_stream,
                &left_write_stream,
                &right_write_stream,
            );

        (left_write_stream, right_write_stream)
    }

    /// Adds a [`OneInTwoOut`] operator that has one input read stream and two output write streams.
    pub fn connect_one_in_two_out<O, S, T, U, V>(
        &self,
        operator_fn: impl Fn() -> O + Clone + Send + Sync + 'static,
        // Add state as an explicit argument to support future features such as state sharing.
        state_fn: impl Fn() -> S + Clone + Send + Sync + 'static,
        config: OperatorConfig,
        read_stream: &dyn Stream<T>,
    ) -> (OperatorStream<U>, OperatorStream<V>)
    where
        O: 'static + OneInTwoOut<S, T, U, V>,
        S: State,
        T: Data + for<'a> Deserialize<'a>,
        U: Data + for<'a> Deserialize<'a>,
        V: Data + for<'a> Deserialize<'a>,
    {
        let left_write_stream = OperatorStream::new(
            &format!("{}-left-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        let right_write_stream = OperatorStream::new(
            &format!("{}-right-write-stream", config.get_name()),
            Arc::clone(&self.internal_graph),
        );
        self.internal_graph.lock().unwrap().connect_one_in_two_out(
            operator_fn,
            state_fn,
            config,
            read_stream,
            &left_write_stream,
            &right_write_stream,
        );

        (left_write_stream, right_write_stream)
    }

    /// Compiles the internal graph representation.
    pub(crate) fn compile(self) -> Result<JobGraph, GraphCompilationError> {
        let mut internal_graph = self.internal_graph.lock().unwrap();
        internal_graph.compile()
    }
}

impl Clone for Graph {
    fn clone(&self) -> Self {
        Self {
            internal_graph: Arc::new(Mutex::new(self.internal_graph.lock().unwrap().clone())),
        }
    }
}
