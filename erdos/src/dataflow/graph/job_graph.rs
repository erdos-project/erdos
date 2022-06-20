use std::{collections::HashMap, fs::File, fmt, io::prelude::*, hash::Hash};

use serde::{Deserialize, Serialize};

use crate::{dataflow::stream::StreamId, OperatorId};

use super::{
    StreamSetupHook, {AbstractOperator, AbstractStreamT, Job}, OperatorRunner,
};

/// The [`InternalGraph`] is an internal representation of the Graph
/// that is communicated to the Leader by the Client / Workers.
// TODO (Sukrit): This should be renamed as AbstractGraph once the
// Graph abstraction is exposed to the developers as GraphBuilder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InternalGraph {
    /// The mapping of all the operators in the Graph from their ID to 
    /// their [`AbstractOperator`] representation.
    operators: HashMap<OperatorId, AbstractOperator>,
    /// A mapping from the ID of the Stream to the ID of the 
    /// AbstractOperator that generates data for it.
    stream_sources: HashMap<StreamId, Job>,
    /// A mapping from the ID of the Stream to the IDs of the
    /// AbstractOperators that consume the data from it.
    stream_destinations: HashMap<StreamId, Vec<Job>>,
}

impl From<JobGraph> for InternalGraph {
    fn from(job_graph: JobGraph) -> Self {
        Self {
            operators: job_graph.operators.clone(),
            stream_sources: job_graph.stream_sources.clone(),
            stream_destinations: job_graph.stream_destinations.clone()
        }
    }
}

#[derive(Clone)]
pub(crate) struct JobGraph {
    operators: HashMap<OperatorId, AbstractOperator>,
    operator_runners: HashMap<OperatorId, Box<dyn OperatorRunner>>,
    streams: HashMap<StreamId, Box<dyn AbstractStreamT>>,
    stream_sources: HashMap<StreamId, Job>,
    stream_destinations: HashMap<StreamId, Vec<Job>>,
    driver_setup_hooks: Vec<Box<dyn StreamSetupHook>>,
}

impl JobGraph {
    pub(crate) fn new(
        operators: HashMap<OperatorId, AbstractOperator>,
        operator_runners: HashMap<OperatorId, Box<dyn OperatorRunner>>,
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

        for operator in operators.values() {
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
            operator_runners,
            // Convert to a HashMap to easily query streams.
            streams: streams.into_iter().map(|s| (s.id(), s)).collect(),
            stream_sources,
            stream_destinations,
            driver_setup_hooks,
        }
    }

    /// Returns a copy of the operators in the graph.
    pub fn operators(&self) -> Vec<AbstractOperator> {
        self.operators.values().cloned().collect()
    }

    /// Retrieve the execution function for a particular operator in the graph.
    pub(crate) fn get_operator_runner(&self, operator_id: &OperatorId) -> Option<Box<dyn OperatorRunner>> {
        match self.operator_runners.get(operator_id) {
            Some(operator_runner) => Some(operator_runner.clone()),
            None => None,
        }
    }

    /// Returns the stream, the stream's source, and the stream's destinations.
    pub fn get_streams(&self) -> Vec<(Box<dyn AbstractStreamT>, Job, Vec<Job>)> {
        self.streams
            .values()
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
        let mut file = File::create(filename)?;
        writeln!(file, "digraph erdos_dataflow {{")?;

        let driver_id = OperatorId::new_deterministic();
        if !self.driver_setup_hooks.is_empty() {
            writeln!(file, "   // Driver")?;
            writeln!(file, "   \"{}\" [label=\"Driver\"];", driver_id)?;
        }

        writeln!(file, "   // Operators")?;
        for operator in self.operators.values() {
            writeln!(
                file,
                "   \"{}\" [label=\"{}\n(Node {})\"];",
                operator.id,
                operator.config.get_name(),
                operator.config.node_id,
            )?;
        }

        writeln!(file, "   // Streams")?;
        for (stream_id, source) in self.stream_sources.iter() {
            let source_id = match source {
                Job::Driver => driver_id,
                Job::Operator(id) => *id,
            };
            if let Some(dests) = self.stream_destinations.get(stream_id) {
                for dest in dests.iter() {
                    let dest_id = match dest {
                        Job::Driver => driver_id,
                        Job::Operator(id) => *id,
                    };
                    let stream_name = self.streams.get(stream_id).unwrap().name();
                    writeln!(
                        file,
                        "   \"{}\" -> \"{}\" [label=\"{}\"];",
                        source_id, dest_id, stream_name
                    )?;
                }
            }
        }

        writeln!(file, "}}")?;
        file.flush()
    }
}

impl fmt::Debug for JobGraph {
    // Outputs an adjacency list representation of the JobGraph.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut adjacency_list = HashMap::new();
        for operator in self.operators.values() {
            let mut dependents = Vec::new();
            for write_stream_id in operator.write_streams.iter() {
                if let Some(write_stream_destinations) = self.stream_destinations.get(write_stream_id) {
                    for dependent_job in write_stream_destinations {
                        match dependent_job {
                            Job::Operator(dependent_job_id) => {
                                if let Some(dependent_job) = self.operators.get(dependent_job_id) {
                                    dependents.push(dependent_job.config.get_name())        
                                }
                            }
                            Job::Driver => dependents.push("Driver".to_string()),
                        }
                    }
                }
            }
            adjacency_list.insert(operator.config.get_name(), dependents);
        }
        write!(f, "{:?}", adjacency_list)
    }
}