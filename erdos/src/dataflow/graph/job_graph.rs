use std::collections::HashMap;

use crate::dataflow::stream::StreamId;

use super::{
    StreamSetupHook, {AbstractOperator, AbstractStreamT, Job},
};

pub(crate) struct JobGraph {
    operators: Vec<AbstractOperator>,
    streams: Vec<Box<dyn AbstractStreamT>>,
    stream_sources: HashMap<StreamId, Job>,
    stream_destinations: HashMap<StreamId, Vec<Job>>,
    driver_setup_hooks: Vec<Box<dyn StreamSetupHook>>,
}

impl JobGraph {
    pub(crate) fn new(
        operators: Vec<AbstractOperator>,
        streams: Vec<Box<dyn AbstractStreamT>>,
        ingest_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
        extract_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
    ) -> Self {
        let mut stream_sources = HashMap::new();
        let mut stream_destinations: HashMap<StreamId, Vec<Job>> = HashMap::new();

        // Initialize stream destination vectors.
        // Necessary because the JobGraph assumes that each stream
        // has a destination vector (may be empty).
        for s in streams.iter() {
            stream_destinations.insert(s.id(), Vec::new());
        }

        for operator in operators.iter() {
            for &read_stream_id in operator.read_streams.iter() {
                stream_destinations
                    .entry(read_stream_id)
                    .or_default()
                    .push(Job::Operator(operator.id));
            }
            for &write_stream_id in operator.write_streams.iter() {
                stream_sources.insert(write_stream_id, Job::Operator(operator.id));
            }
        }

        let mut driver_setup_hooks = Vec::new();
        for (ingest_stream_id, setup_hook) in ingest_streams {
            stream_sources.insert(ingest_stream_id, Job::Driver);
            driver_setup_hooks.push(setup_hook);
        }

        for (extract_stream_id, setup_hook) in extract_streams {
            stream_destinations
                .entry(extract_stream_id)
                .or_default()
                .push(Job::Driver);
            driver_setup_hooks.push(setup_hook);
        }

        Self {
            operators,
            streams,
            stream_sources,
            stream_destinations,
            driver_setup_hooks,
        }
    }

    /// Returns a copy of the operators in the graph.
    pub fn operators(&self) -> Vec<AbstractOperator> {
        self.operators.clone()
    }

    /// Returns the stream, the stream's source, and the stream's destinations.
    pub fn get_streams(&self) -> Vec<(Box<dyn AbstractStreamT>, Job, Vec<Job>)> {
        self.streams
            .iter()
            .map(|s| {
                let source = *self.stream_sources.get(&s.id()).unwrap_or_else(|| {
                    panic!(
                        "Internal error: stream {} (ID: {}) must have a source",
                        s.name(),
                        s.id()
                    )
                });
                let destinations = self
                    .stream_destinations
                    .get(&s.id())
                    .cloned()
                    .unwrap_or_else(|| {
                        panic!(
                            "Internal error: stream {} (ID: {}) must have a destination",
                            s.name(),
                            s.id()
                        )
                    });
                (s.box_clone(), source, destinations)
            })
            .collect()
    }

    /// Returns the hooks used to set up ingest and extract streams.
    pub fn get_driver_setup_hooks(&self) -> Vec<Box<dyn StreamSetupHook>> {
        let mut driver_setup_hooks = Vec::new();
        for i in 0..self.driver_setup_hooks.len() {
            driver_setup_hooks.push(self.driver_setup_hooks[i].box_clone());
        }
        driver_setup_hooks
    }

    /// Exports the job graph to a Graphviz file (*.gv, *.dot).
    pub fn to_graph_viz(&self, filename: &str) -> std::io::Result<()> {
        todo!("Implement to_graph_viz to export file {}", filename)
    }
}
