use std::collections::HashMap;

use crate::{
    dataflow::graph::{InternalGraph, Job},
    node::{WorkerId, WorkerState},
};

// Public exports
pub mod channel_manager;

pub(crate) trait JobGraphScheduler {
    fn schedule_graph(
        &mut self,
        job_graph: &InternalGraph,
        workers: &Vec<WorkerState>,
    ) -> HashMap<Job, WorkerId>;
}

// TODO (Sukrit): Implement more schedulers and move these implementations
// to a separate module.
/// The [`SimpleJobGraphScheduler`] maps the operators to the nodes they
/// were requested to be run on in their [`OperatorConfig`].
pub(crate) struct SimpleJobGraphScheduler {}

impl Default for SimpleJobGraphScheduler {
    fn default() -> Self {
        Self {}
    }
}

impl JobGraphScheduler for SimpleJobGraphScheduler {
    fn schedule_graph(
        &mut self,
        job_graph: &InternalGraph,
        workers: &Vec<WorkerState>,
    ) -> HashMap<Job, WorkerId> {
        let mut placements = HashMap::new();

        for (operator_id, operator) in job_graph.get_operators().iter() {
            let requested_node = operator.config.node_id;
            placements.insert(operator_id.clone(), requested_node.into());
        }

        placements
    }
}
