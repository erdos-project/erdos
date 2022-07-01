use std::{collections::HashMap, fmt, fs::File, io::prelude::*};

use serde::{Deserialize, Serialize};

use crate::{dataflow::stream::StreamId, OperatorId};

use super::{
    JobGraphId, OperatorRunner, StreamSetupHook, {AbstractOperator, AbstractStreamT, Job},
};

/// The [`InternalGraph`] is an internal representation of the Graph
/// that is communicated to the Leader by the Client / Workers.
// TODO (Sukrit): This should be renamed as AbstractGraph once the
// Graph abstraction is exposed to the developers as GraphBuilder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AbstractJobGraph {
    /// The name of the JobGraph that this graph is a representation of.
    name: String,
    /// The mapping of all the operators in the Graph from their ID to
    /// their [`AbstractOperator`] representation.
    operators: HashMap<Job, AbstractOperator>,
    /// A mapping from the ID of the Stream to the ID of the
    /// AbstractOperator that generates data for it.
    stream_sources: HashMap<StreamId, Job>,
    /// A mapping from the ID of the Stream to the IDs of the
    /// AbstractOperators that consume the data from it.
    stream_destinations: HashMap<StreamId, Vec<Job>>,
    /// A collection of IDs of the IngressStreams.
    ingress_streams: Vec<StreamId>,
    /// A collection of IDs of the EgressStreams.
    egress_streams: Vec<StreamId>,
}

impl AbstractJobGraph {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Retrieve the [`AbstractOperator`] underlying the given [`Job`]
    ///
    /// Returns None if the Job was of variant [`Driver`] or the job was
    /// not found in the graph.
    pub(crate) fn operator(&self, job: &Job) -> Option<AbstractOperator> {
        self.operators.get(job).cloned()
    }

    /// Retrieve the collection of [`AbstractOperator`]s in the Graph.
    pub(crate) fn operators(&self) -> &HashMap<Job, AbstractOperator> {
        &self.operators
    }

    /// Retrieve the [`Job`] that publishes the data on the given stream_id.
    pub(crate) fn source(&self, stream_id: &StreamId) -> Option<Job> {
        self.stream_sources.get(stream_id).cloned()
    }

    pub(crate) fn destinations(&self, stream_id: &StreamId) -> Vec<Job> {
        match self.stream_destinations.get(stream_id) {
            Some(destinations) => destinations.clone(),
            None => Vec::new(),
        }
    }

    pub(crate) fn ingress_streams(&self) -> Vec<StreamId> {
        self.ingress_streams.clone()
    }

    pub(crate) fn egress_streams(&self) -> Vec<StreamId> {
        self.egress_streams.clone()
    }
}

impl From<JobGraph> for AbstractJobGraph {
    fn from(job_graph: JobGraph) -> Self {
        let mut sources = HashMap::new();
        let mut destinations = HashMap::new();

        for (stream_id, stream) in job_graph.streams.iter() {
            sources.insert(*stream_id, stream.source().unwrap());
            destinations.insert(*stream_id, stream.destinations());
        }
        Self {
            name: job_graph.name.clone(),
            operators: job_graph.operators.clone(),
            stream_sources: sources,
            stream_destinations: destinations,
            ingress_streams: job_graph.ingress_streams.keys().cloned().collect(),
            egress_streams: job_graph.egress_streams.keys().cloned().collect(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct JobGraph {
    id: JobGraphId,
    name: String,
    operators: HashMap<Job, AbstractOperator>,
    operator_runners: HashMap<OperatorId, Box<dyn OperatorRunner>>,
    streams: HashMap<StreamId, Box<dyn AbstractStreamT>>,
    ingress_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
    egress_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
}

impl JobGraph {
    pub(crate) fn new(
        name: String,
        operators: HashMap<Job, AbstractOperator>,
        operator_runners: HashMap<OperatorId, Box<dyn OperatorRunner>>,
        streams: HashMap<StreamId, Box<dyn AbstractStreamT>>,
        ingress_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
        egress_streams: HashMap<StreamId, Box<dyn StreamSetupHook>>,
    ) -> Self {
        Self {
            id: JobGraphId(name.clone()),
            name,
            operators,
            operator_runners,
            streams,
            ingress_streams,
            egress_streams,
        }
    }

    /// Retreives the name of the JobGraph.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Retrieves the ID of the JobGraph.
    pub(crate) fn id(&self) -> JobGraphId {
        self.id.clone()
    }

    /// Returns a copy of the operators in the graph.
    pub fn operators(&self) -> Vec<AbstractOperator> {
        self.operators.values().cloned().collect()
    }

    /// Retrieve the [`AbstractOperator`] for the given [`Job`].
    pub(crate) fn get_job(&self, job: &Job) -> Option<AbstractOperator> {
        self.operators.get(job).cloned()
    }

    /// Retrieve the execution function for a particular operator in the graph.
    pub(crate) fn get_operator_runner(
        &self,
        operator_id: &OperatorId,
    ) -> Option<Box<dyn OperatorRunner>> {
        match self.operator_runners.get(operator_id) {
            Some(operator_runner) => Some(operator_runner.clone()),
            None => None,
        }
    }

    /// Returns the streams of this JobGraph.
    pub fn get_streams(&self) -> Vec<Box<dyn AbstractStreamT>> {
        self.streams.values().into_iter().cloned().collect()
    }

    pub fn get_stream(&self, stream_id: &StreamId) -> Option<Box<dyn AbstractStreamT>> {
        self.streams.get(stream_id).cloned()
    }

    /// Returns the [`AbstractStream`] representations of the [`IngressStream`]s
    /// registered in the [`JobGraph`].
    pub fn ingress_streams(&self) -> Vec<Box<dyn AbstractStreamT>> {
        let mut ingress_streams = Vec::new();
        for (ingress_stream_id, _) in &self.ingress_streams {
            if let Some(ingress_stream) = self.get_stream(&ingress_stream_id) {
                ingress_streams.push(ingress_stream);
            }
        }
        ingress_streams
    }

    /// Returns the [`AbstractStream`] representations of the [`EgressStream`]s
    /// registered in the [`JobGraph`].
    pub fn egress_streams(&self) -> Vec<Box<dyn AbstractStreamT>> {
        let mut egress_streams = Vec::new();
        for (egress_stream_id, _) in &self.egress_streams {
            if let Some(egress_stream) = self.get_stream(&egress_stream_id) {
                egress_streams.push(egress_stream);
            }
        }
        egress_streams
    }

    /// Returns the hooks used to set up ingress and egress streams.
    pub fn get_driver_setup_hooks(&self) -> Vec<Box<dyn StreamSetupHook>> {
        let mut driver_setup_hooks = Vec::new();
        for (_, setup_hook) in &self.ingress_streams {
            driver_setup_hooks.push(setup_hook.box_clone());
        }
        for (_, setup_hook) in &self.egress_streams {
            driver_setup_hooks.push(setup_hook.clone());
        }
        driver_setup_hooks
    }

    /// Exports the job graph to a Graphviz file (*.gv, *.dot).
    pub fn to_graph_viz(&self, filename: &str) -> std::io::Result<()> {
        let mut file = File::create(filename)?;
        writeln!(file, "digraph erdos_dataflow {{")?;

        let driver_id = OperatorId::new_deterministic();
        // if !self.driver_setup_hooks.is_empty() {
        //     writeln!(file, "   // Driver")?;
        //     writeln!(file, "   \"{}\" [label=\"Driver\"];", driver_id)?;
        // }

        writeln!(file, "   // Operators")?;
        for operator in self.operators.values() {
            writeln!(
                file,
                "   \"{}\" [label=\"{}\n(Node {})\"];",
                operator.id,
                operator.config.get_name(),
                operator.config.worker_id,
            )?;
        }

        writeln!(file, "   // Streams")?;
        for stream in self.streams.values().into_iter() {
            let source_id = match stream.source().unwrap() {
                Job::Driver => driver_id,
                Job::Operator(id) => id,
            };
            for destination in stream.destinations().iter() {
                let destination_id = match destination {
                    Job::Driver => driver_id,
                    Job::Operator(id) => *id,
                };
                let stream_name = stream.name();
                writeln!(
                    file,
                    "   \"{}\" -> \"{}\" [label=\"{}\"];",
                    source_id, destination_id, stream_name
                )?;
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
                let write_stream = self.streams.get(write_stream_id).unwrap();
                for dependent_job in write_stream.destinations() {
                    match dependent_job {
                        Job::Operator(dependent_job_id) => {
                            if let Some(dependent_job) =
                                self.operators.get(&Job::Operator(dependent_job_id))
                            {
                                dependents.push(dependent_job.config.get_name())
                            }
                        }
                        Job::Driver => dependents.push("Driver".to_string()),
                    }
                }
            }
            adjacency_list.insert(operator.config.get_name(), dependents);
        }
        write!(f, "{:?}", adjacency_list)
    }
}
