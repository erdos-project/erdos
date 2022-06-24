use std::sync::{Arc, Mutex};

use serde::Deserialize;

use crate::{
    dataflow::{
        operator::{
            OneInOneOut, OneInTwoOut, ParallelOneInOneOut, ParallelOneInTwoOut, ParallelSink,
            ParallelTwoInOneOut, Sink, Source, TwoInOneOut,
        },
        stream::{ExtractStream, IngestStream, OperatorStream},
        AppendableState, Data, LoopStream, State, Stream,
    },
    OperatorConfig,
};

use super::{InternalGraph, JobGraph};

pub struct Graph {
    internal_graph: Arc<Mutex<InternalGraph>>,
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

impl Graph {
    pub fn new() -> Self {
        Self {
            internal_graph: Arc::new(Mutex::new(InternalGraph::new())),
        }
    }

    pub fn add_ingest_stream<D>(&self, name: &str) -> IngestStream<D>
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let ingest_stream = IngestStream::new(name, Arc::clone(&self.internal_graph));
        self.internal_graph
            .lock()
            .unwrap()
            .add_ingest_stream(&ingest_stream);

        ingest_stream
    }

    pub fn extract<D>(&self, stream: &OperatorStream<D>) -> ExtractStream<D>
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let extract_stream = ExtractStream::new(stream);
        self.internal_graph
            .lock()
            .unwrap()
            .add_extract_stream(&extract_stream);

        extract_stream
    }

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

    pub(crate) fn compile(&self) -> JobGraph {
        self.internal_graph.lock().unwrap().compile()
    }
}
