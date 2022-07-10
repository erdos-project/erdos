use std::collections::{HashMap, HashSet};

use rand::{thread_rng, Rng};

use crate::{
    dataflow::graph::{AbstractJobGraph, Job},
    node::{WorkerId, WorkerState},
};

pub(crate) trait JobGraphScheduler {
    fn schedule_graph(
        &mut self,
        job_graph: &AbstractJobGraph,
        workers: &[WorkerState],
    ) -> HashMap<Job, WorkerId>;
}

// TODO (Sukrit): Implement more schedulers and move these implementations
// to a separate module.
/// The [`SimpleJobGraphScheduler`] maps the operators to the nodes they
/// were requested to be run on in their [`OperatorConfig`].
#[derive(Default)]
pub(crate) struct SimpleJobGraphScheduler {}

impl JobGraphScheduler for SimpleJobGraphScheduler {
    fn schedule_graph(
        &mut self,
        job_graph: &AbstractJobGraph,
        workers: &[WorkerState],
    ) -> HashMap<Job, WorkerId> {
        if workers.is_empty() {
            // No workers found, we cannot schedule anything.
            return HashMap::new();
        }

        // Calculate the requested placement, and assign a random Worker if the
        // requested Worker was not found.
        let mut placements = HashMap::new();
        let worker_ids: HashSet<_> = workers
            .iter()
            .map(|worker_state| worker_state.id())
            .collect();

        for (operator_id, operator) in job_graph.operators().iter() {
            let requested_worker = operator.config.worker_id;
            if worker_ids.contains(&requested_worker) {
                placements.insert(*operator_id, requested_worker);
            } else {
                // Assign a random worker.
                let worker_index = thread_rng().gen_range(0, worker_ids.len());
                let worker_id = workers.get(worker_index).unwrap().id();
                placements.insert(*operator_id, worker_id);
            }
        }

        placements
    }
}
