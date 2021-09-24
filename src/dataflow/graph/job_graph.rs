use std::collections::HashMap;

use crate::{dataflow::stream::StreamId, OperatorId};

use super::{
    abstract_graph::{AbstractOperator, AbstractStreamT},
    StreamSetupHook,
};

pub(crate) enum Job {
    Operator(OperatorId),
    Driver,
}

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
}
