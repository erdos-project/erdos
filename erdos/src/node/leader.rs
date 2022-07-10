//! Abstractions that centralize the control plane of execution of
//! [`Graph`](crate::dataflow::graph::Graph)s from
//! [`Worker`](crate::node::worker_node::WorkerNode)s.
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use futures::{future, stream::SplitSink, SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        broadcast,
        mpsc::{self, Receiver, UnboundedSender},
    },
    task::JoinHandle,
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        control_plane::{notifications::*, ControlPlaneCodec},
        errors::CommunicationError,
    },
    dataflow::graph::{AbstractJobGraph, Job, JobGraphId},
    scheduler::{JobGraphScheduler, SimpleJobGraphScheduler},
};

use super::{ExecutionState, WorkerId, WorkerState};

/// The notifications that are communicated between the main `Leader` thread
/// and the individual tasks spawned for handling each of the attached `Worker`s.
#[derive(Debug, Clone)]
enum WorkerHandlerNotification {
    WorkerInitialized(WorkerState),
    ScheduleJobGraph(WorkerId, JobGraphId, AbstractJobGraph),
    ScheduleJob(JobGraphId, Job, WorkerId, HashMap<Job, WorkerAddress>),
    JobUpdate(JobGraphId, Job, ExecutionState),
    ExecuteGraph(JobGraphId, HashSet<WorkerId>),
    Query(WorkerId, QueryId, QueryType),
    QueryResponse(WorkerId, QueryId, QueryResponseType),
    Shutdown(WorkerId),
    ShutdownAllWorkers,
}

struct JobGraphMetadata {
    /// The ID of the [`JobGraph`] for which this metadata is maintained.
    pub id: JobGraphId,
    /// An abstract version of the [`JobGraph`] for which this metadata is maintained.
    pub job_graph: AbstractJobGraph,
    /// A representation of the state that the [`JobGraph`] is in.
    pub job_graph_state: ExecutionState,
    /// A mapping from the [`Job`] to the [`Worker`] it is currently assigned to, along
    /// with its execution.
    pub job_status: HashMap<Job, (Option<WorkerId>, ExecutionState)>,
}

impl JobGraphMetadata {
    pub fn new(
        id: JobGraphId,
        job_graph: AbstractJobGraph,
        job_graph_state: ExecutionState,
        job_status: HashMap<Job, (Option<WorkerId>, ExecutionState)>,
    ) -> Self {
        Self {
            id,
            job_graph,
            job_graph_state,
            job_status,
        }
    }

    pub fn update_job_state(&mut self, job: &Job, state: ExecutionState) -> bool {
        if let Some(job_status) = self.job_status.get_mut(job) {
            job_status.1 = state;
            true
        } else {
            false
        }
    }

    pub fn update_all_job_states(&mut self, state: ExecutionState) {
        let jobs: Vec<_> = self.job_status.keys().cloned().collect();
        for job in jobs {
            self.update_job_state(&job, state.clone());
        }
    }

    pub fn all_jobs_in_state(&self, state: ExecutionState) -> bool {
        self.job_status
            .values()
            .map(|(_, job_state)| job_state)
            .all(|job_state| *job_state == state)
    }

    pub fn assigned_workers(&self) -> HashSet<WorkerId> {
        self.job_status
            .values()
            .filter_map(|(worker_id, _)| *worker_id)
            .collect()
    }
}

/// An abstraction that centralizes the management of the execution of ERDOS [`Graph`]s.
///
/// A `Leader` forms a centralized service that allows `Worker`s to discover each other,
/// and submit `Graph`s for scheduling, that are then scheduled and executed across the
/// available `Worker`s.
pub(crate) struct Leader {
    /// The address that the LeaderNode binds to.
    address: SocketAddr,
    /// A Receiver corresponding to a channel between the Driver and the Leader.
    driver_notification_rx: Receiver<DriverNotification>,
    /// A Vector containing the handlers corresponding to each Worker.
    worker_handlers: Vec<JoinHandle<()>>,
    /// A mapping between the ID of the Worker and the state maintained for it.
    worker_id_to_worker_state: HashMap<WorkerId, WorkerState>,
    /// The scheduler to be used for scheduling JobGraphs onto Workers.
    job_graph_scheduler: Box<dyn JobGraphScheduler + Send>,
    /// A mapping between the JobGraph and the [`JobGraphMetadata`] stored by the Leader.
    job_graph_metadata: HashMap<JobGraphId, JobGraphMetadata>,
}

impl Leader {
    /// Initialize a [`Leader`] at the given `address` and with the provided channel
    /// to the [`Driver`](crate::dataflow::graph::Job::Driver).
    ///
    /// To actually bind to the address, and execute the computation, invoke [`run`](self.run).
    ///
    /// # Arguments
    /// * `address`: The address where the [`Leader`] will bind to.
    /// * `driver_notification_rx`: A channel where the `Driver` may send notifications.
    pub(crate) fn new(
        address: SocketAddr,
        driver_notification_rx: Receiver<DriverNotification>,
    ) -> Self {
        Self {
            address,
            driver_notification_rx,
            worker_handlers: Vec::new(),
            worker_id_to_worker_state: HashMap::new(),
            // TODO (Sukrit): The type of Scheduler should be chosen by a Configuration.
            job_graph_scheduler: Box::new(SimpleJobGraphScheduler::default()),
            job_graph_metadata: HashMap::new(),
        }
    }

    /// Initialize the [`Leader`] and begin listening for notifications from [`Worker`]s.
    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        let leader_tcp_listener = TcpListener::bind(self.address).await?;
        let (workers_to_leader_tx, mut workers_to_leader_rx) = mpsc::unbounded_channel();
        let (leader_to_workers_tx, _) = broadcast::channel(100);

        loop {
            tokio::select! {
                // Handle new Worker connections.
                worker_connection = leader_tcp_listener.accept() => {
                    match worker_connection {
                        Ok((worker_stream, worker_address)) => {
                            // Create channels between Worker handler and Leader.
                            // Spawn a task to handle the Worker connection.
                            tracing::debug!(
                                "[Leader] Received a Worker connection from address: {}",
                                worker_address
                            );
                            let leader_to_worker_broadcast_channel =
                                leader_to_workers_tx.subscribe();
                            let worker_handler = tokio::spawn(Leader::handle_worker(
                                worker_stream,
                                workers_to_leader_tx.clone(),
                                leader_to_worker_broadcast_channel,
                            ));
                            self.worker_handlers.push(worker_handler);
                        }
                        Err(error) => {
                            tracing::error!(
                                "[Leader] Received an error when handling a Worker connection: {}",
                                error
                            );
                        }
                    }
                }

                // Handle new messages from the drivers.
                Some(driver_notification) = self.driver_notification_rx.recv() => {
                    if let DriverNotification::Shutdown = driver_notification {
                        // Ask all Workers to shutdown.
                        tracing::debug!(
                            "[Leader] Received a Shutdown notification from the Driver. \
                                            Requesting all the Workers to shutdown."
                        );
                        if let Err(error) =
                            leader_to_workers_tx.send(
                                WorkerHandlerNotification::ShutdownAllWorkers
                            ) {
                            tracing::error!(
                                "[Leader] Received an error when requesting \
                                                    Worker shutdown: {}",
                                error
                            );
                        }

                        // Wait for all worker handler tasks to shutdown.
                        future::join_all(self.worker_handlers.drain(..)).await;
                        tracing::info!("Leader is shutting down!");
                        return Ok(());
                    }
                }

                // Handle new messages from the Worker handlers.
                Some(worker_handler_msg) = workers_to_leader_rx.recv() =>  {
                    self.handle_worker_message(worker_handler_msg, &leader_to_workers_tx);
                }
            }
        }
    }

    /// Reacts to the messages relayed from the Worker to the Leader by the
    /// threads running the `handle_worker` method for each `Worker`.
    fn handle_worker_message(
        &mut self,
        worker_handler_msg: WorkerHandlerNotification,
        leader_to_workers_tx: &broadcast::Sender<WorkerHandlerNotification>,
    ) {
        match worker_handler_msg {
            WorkerHandlerNotification::WorkerInitialized(worker_state) => {
                // Install the state of the Worker in the Leader.
                self.worker_id_to_worker_state
                    .insert(worker_state.id(), worker_state);
            }
            WorkerHandlerNotification::ScheduleJobGraph(driver_id, job_graph_id, job_graph) => {
                // We consider the Worker scheduling the JobGraph to be the Driver.
                self.schedule_graph(driver_id, job_graph_id, job_graph, leader_to_workers_tx);
            }
            WorkerHandlerNotification::JobUpdate(job_graph_id, job, job_state) => {
                if job_state == ExecutionState::Ready {
                    // Mark the job as ready, and if this was the last Job that required scheduling,
                    // then notify all of the Workers that the JobGraph was placed on to execute
                    // their jobs.
                    self.mark_job_ready(job_graph_id, job, leader_to_workers_tx);
                } else {
                    // TODO (Sukrit): Expand the set of states and updates.
                }
            }
            WorkerHandlerNotification::Shutdown(worker_id) => {
                self.worker_id_to_worker_state.remove(&worker_id);
            }
            WorkerHandlerNotification::Query(worker_id, query_id, query) => {
                if let Err(error) = match &query {
                    QueryType::JobGraphStatus(job_graph_id) => {
                        let response = match self.job_graph_metadata.get(job_graph_id) {
                            Some(graph_metadata) => Ok(graph_metadata.job_graph_state.clone()),
                            None => Err(QueryError(format!(
                                "JobGraph {:?} was not registered in the Leader.",
                                job_graph_id,
                            ))),
                        };
                        leader_to_workers_tx.send(WorkerHandlerNotification::QueryResponse(
                            worker_id,
                            query_id.clone(),
                            QueryResponseType::JobGraphStatus(job_graph_id.clone(), response),
                        ))
                    }
                } {
                    tracing::warn!(
                        "[Leader] Error responding to the Query({:?})[{:?}] by the Worker {}: {:?}",
                        query_id,
                        query,
                        worker_id,
                        error,
                    );
                }
            }
            // The rest of the notifications are to be sent from the Leader to the WorkerHandlers.
            _ => unreachable!(),
        }
    }

    /// A task that relays messages from the [`Leader`] to a specific [`Worker`] and vice-versa.
    ///
    /// # Arguments
    /// * `worker_stream`: The TCP stream that was initiated by the `Worker`, and where it will
    ///    send messages to the `Leader`, and receive messages from it.
    /// * `channel_to_leader`: An MPSC channel to send messages to the `Leader` shared across all
    ///    the handlers for each registered `Worker`.
    /// * `channel_from_leader`: A broadcast channel for the `Leader` to send notifications tagged
    ///    for a particular `Worker` to the handler.
    async fn handle_worker(
        worker_stream: TcpStream,
        channel_to_leader: UnboundedSender<WorkerHandlerNotification>,
        mut channel_from_leader: broadcast::Receiver<WorkerHandlerNotification>,
    ) {
        let (mut worker_tx, mut worker_rx) = Framed::new(
            worker_stream,
            ControlPlaneCodec::<LeaderNotification, WorkerNotification>::default(),
        )
        .split();

        let mut id_of_this_worker = WorkerId::default();

        fn handle_messages_from_worker(
            msg_from_worker: WorkerNotification,
            id_of_this_worker: WorkerId,
            channel_to_leader: &UnboundedSender<WorkerHandlerNotification>,
        ) {
            match msg_from_worker {
                WorkerNotification::JobUpdate(job_graph_id, job, job_state) => {
                    tracing::trace!(
                        "[WorkerHandler {}] Job {:?} from JobGraph {:?} is in state {:?}.",
                        id_of_this_worker,
                        job,
                        job_graph_id,
                        job_state,
                    );
                    if let Err(error) = channel_to_leader.send(
                        WorkerHandlerNotification::JobUpdate(job_graph_id, job, job_state.clone()),
                    ) {
                        tracing::warn!(
                            "[WorkerHandler {}] Error notifying Leader of a status \
                                            update for Job {:?} to {:?}: {}",
                            id_of_this_worker,
                            job,
                            job_state,
                            error,
                        );
                    }
                }
                WorkerNotification::SubmitGraph(job_graph_id, job_graph) => {
                    tracing::trace!(
                        "[WorkerHandler {}] Received the Graph {} (ID={:?}) \
                                                                for scheduling.",
                        id_of_this_worker,
                        job_graph.name(),
                        job_graph_id,
                    );
                    if let Err(error) =
                        channel_to_leader.send(WorkerHandlerNotification::ScheduleJobGraph(
                            id_of_this_worker,
                            job_graph_id.clone(),
                            job_graph,
                        ))
                    {
                        tracing::warn!(
                            "[WorkerHandler {}] Error submitting the Graph {:?} \
                                                                to the Leader: {}",
                            id_of_this_worker,
                            job_graph_id,
                            error,
                        );
                    }
                }
                WorkerNotification::Query(query_id, query) => {
                    tracing::trace!(
                        "[WorkerHandler {}] Received the Query({:?}): {:?}.",
                        id_of_this_worker,
                        query_id,
                        query,
                    );
                    if let Err(error) = channel_to_leader.send(WorkerHandlerNotification::Query(
                        id_of_this_worker,
                        query_id.clone(),
                        query.clone(),
                    )) {
                        tracing::warn!(
                            "[WorkerHandler {}] Error submitting the \
                                Query({:?})[{:?}] to the Leader: {}",
                            id_of_this_worker,
                            query_id,
                            query,
                            error,
                        );
                    }
                }
                // Initialization and Shutdown should be handled in the main loop.
                WorkerNotification::Shutdown | WorkerNotification::Initialized(_, _, _) => {
                    unreachable!()
                }
            }
        }

        async fn handle_messages_from_leader(
            msg_from_leader: WorkerHandlerNotification,
            id_of_this_worker: WorkerId,
            worker_tx: &mut SplitSink<
                Framed<TcpStream, ControlPlaneCodec<LeaderNotification, WorkerNotification>>,
                LeaderNotification,
            >,
        ) {
            match msg_from_leader {
                WorkerHandlerNotification::ScheduleJob(
                    job_graph_id,
                    job,
                    worker_id,
                    worker_addresses,
                ) => {
                    // The Leader assigns an operator to a worker.
                    if id_of_this_worker == worker_id {
                        tracing::trace!(
                            "[WorkerHandler {}] Scheduling Job {:?} from JobGraph {:?}.",
                            id_of_this_worker,
                            job,
                            job_graph_id,
                        );
                        if let Err(error) = worker_tx
                            .send(LeaderNotification::ScheduleJob(
                                job_graph_id,
                                job,
                                worker_addresses,
                            ))
                            .await
                        {
                            tracing::warn!(
                                "[WorkerHandler {}] Error notifying Worker \
                                        of the scheduled Job {:?}: {:?}",
                                id_of_this_worker,
                                job,
                                error,
                            );
                        }
                    }
                }
                WorkerHandlerNotification::ExecuteGraph(job_graph_id, worker_addresses) => {
                    // Tell the Worker to execute the operators for this graph.
                    if worker_addresses.contains(&id_of_this_worker) {
                        tracing::debug!(
                            "[WorkerHandler {}] Notifiying Worker to execute the JobGraph {:?}.",
                            id_of_this_worker,
                            job_graph_id,
                        );
                        if let Err(error) = worker_tx
                            .send(LeaderNotification::ExecuteGraph(job_graph_id.clone()))
                            .await
                        {
                            tracing::warn!(
                                "[WorkerHandler {}] Error notifying Worker to \
                                            execute JobGraph {:?}: {:?}",
                                id_of_this_worker,
                                job_graph_id,
                                error,
                            )
                        }
                    }
                }
                WorkerHandlerNotification::QueryResponse(worker_id, query_id, query_response) => {
                    // The Leader responds to a Query by a Worker.
                    if id_of_this_worker == worker_id {
                        tracing::trace!(
                            "[WorkerHandler {}] Responding with a QueryResponse({:?}) \
                                                            {:?} from Leader.",
                            id_of_this_worker,
                            query_id,
                            query_response,
                        );
                        if let Err(error) = worker_tx
                            .send(LeaderNotification::QueryResponse(
                                query_id.clone(),
                                query_response.clone(),
                            ))
                            .await
                        {
                            tracing::warn!(
                                "[WorkerHandler {}] Error notifying Worker of the \
                                    QueryResponse({:?})[{:?}] from Leader: {:?}",
                                id_of_this_worker,
                                query_id,
                                query_response,
                                error,
                            );
                        }
                    }
                }
                _ => {}
            }
        }

        // Handle messages from the Worker and the Leader.
        loop {
            tokio::select! {
                // Communicate messages received from the Worker to the Leader.
                Some(Ok(msg_from_worker)) = worker_rx.next() => {
                    match msg_from_worker {
                        WorkerNotification::Initialized(worker_id, worker_address, resources) => {
                            id_of_this_worker = worker_id;
                            // Communicate the Worker ID to the Leader.
                            tracing::debug!(
                                "[WorkerHandler {}] Initialized a connection to the Worker at {}.",
                                id_of_this_worker,
                                worker_address,
                            );
                            if let Err(error) =
                                channel_to_leader.send(WorkerHandlerNotification::WorkerInitialized(
                                    WorkerState::new(
                                        id_of_this_worker,
                                        worker_address,
                                        resources,
                                    )
                                ))
                            {
                                tracing::warn!(
                                    "[WorkerHandler {}] Error notifying Leader of the \
                                            initialization state of a new Worker: {}",
                                    id_of_this_worker,
                                    error,
                                );
                            }
                        }
                        WorkerNotification::Shutdown => {
                            tracing::info!(
                                "[WorkerHandler {}] Worker is shutting down.",
                                id_of_this_worker
                            );
                            if let Err(error) = channel_to_leader
                                                .send(WorkerHandlerNotification::Shutdown(
                                                        id_of_this_worker)
                                                )
                            {
                                tracing::warn!(
                                    "[WorkerHandler {}] Error notifying Leader of \
                                                    the Worker's shutdown: {}",
                                    id_of_this_worker,
                                    error,
                                );
                            }
                            return;
                        }
                        _ => handle_messages_from_worker(
                            msg_from_worker,
                            id_of_this_worker,
                            &channel_to_leader,
                        ),
                    }
               }

                // Communicate messages received from the Leader to the Worker.
                Ok(msg_from_leader) = channel_from_leader.recv() => {
                    match msg_from_leader {
                        WorkerHandlerNotification::ShutdownAllWorkers => {
                            // The Leader requested all nodes to shutdown.
                            let _ = worker_tx.send(LeaderNotification::Shutdown).await;
                            tracing::debug!(
                                "[WorkerHandler {}] Worker was requested to shutdown.",
                                id_of_this_worker
                            );
                            return;
                        }
                        _ => handle_messages_from_leader(
                            msg_from_leader,
                            id_of_this_worker,
                            &mut worker_tx,
                        ).await,
                    }
                }
            }
        }
    }

    /// Retrieves the address of the `Worker` where the `job` was placed by the scheduler.
    ///
    /// # Arguments
    /// * `worker_id`: The ID of the Worker to whom the address needs to be communicated.
    /// * `driver_id`: The ID of the Worker that submitted the JobGraph for execution.
    /// * `job`: The `Job` whose originating address needs to be retrieved.
    /// * `placements`: The mapping from the `job` to the WorkerId where it was scheduled.
    fn get_worker_address(
        &self,
        worker_id: &WorkerId,
        driver_id: &WorkerId,
        job: &Job,
        placements: &HashMap<Job, WorkerId>,
    ) -> Option<WorkerAddress> {
        Some(match job {
            // If the job whose address needs to be retrieved is an operator, then
            // find the Worker where the Operator was placed, and retrieve its address.
            Job::Operator(_) => {
                let worker_id_for_operator = *placements.get(job)?;
                if worker_id_for_operator == *worker_id {
                    WorkerAddress::Local
                } else {
                    // Find the address of the Worker node.
                    let worker_address = self
                        .worker_id_to_worker_state
                        .get(&worker_id_for_operator)?
                        .address();
                    WorkerAddress::Remote(worker_id_for_operator, worker_address)
                }
            }
            // If the job whose address needs to be retrieved is the driver, then
            // we just query the WorkerState to find its address.
            Job::Driver => {
                if driver_id == worker_id {
                    // NOTE: This should never happen since no jobs should be placed by
                    // the Scheduler on the Driver, but we let this remain to ensure that
                    // simplistic setups with the placements pre-specified using
                    // Configurations still work.
                    WorkerAddress::Local
                } else {
                    let driver_address = self.worker_id_to_worker_state.get(driver_id)?.address();
                    WorkerAddress::Remote(*driver_id, driver_address)
                }
            }
        })
    }

    /// Schedules the given [`AbstractJobGraph`] across the set of registered [`Worker`]s.
    ///
    /// # Arguments
    /// * `driver_id`: The ID of the Worker that submitted the [`AbstractJobGraph`].
    /// * `job_graph_id`: The ID of the [`AbstractJobGraph`] to be scheduled.
    /// * `job_graph`: The [`AbstractJobGraph`] instance to be scheduled across the workers.
    /// * `leader_to_workers_tx`: A channel to communicate the `ScheduleJob` notification to
    ///     each of the tasks handling the connections to the `Worker`s.
    fn schedule_graph(
        &mut self,
        driver_id: WorkerId,
        job_graph_id: JobGraphId,
        job_graph: AbstractJobGraph,
        leader_to_workers_tx: &broadcast::Sender<WorkerHandlerNotification>,
    ) {
        // Invoke the Scheduler to retrieve the placements for this JobGraph.
        let workers: Vec<_> = self.worker_id_to_worker_state.values().cloned().collect();
        let placements = self
            .job_graph_scheduler
            .schedule_graph(&job_graph, &workers);

        // Maintain the state of each of the `Job`s in the graph.
        let mut job_status = HashMap::new();

        // Request the thread handling the `Worker` where the `Operator` was placed to schedule it.
        for (job, worker_id) in placements.iter() {
            let operator = match job_graph.operator(job) {
                Some(operator) => operator,
                None => {
                    tracing::warn!(
                        "[Leader] The scheduler returned a placement for the Job {:?} for \
                        which a corresponding operator was not found in the JobGraph {:?}.",
                        job,
                        job_graph_id,
                    );
                    continue;
                }
            };
            let mut worker_addresses = HashMap::new();

            // For all the ReadStreams of this operator, let the Worker executing it
            // know the addresses of the source operator of the stream.
            for read_stream_id in &operator.read_streams {
                match job_graph.source(read_stream_id) {
                    Some(job) => {
                        if let Some(worker_address) =
                            self.get_worker_address(worker_id, &driver_id, &job, &placements)
                        {
                            worker_addresses.insert(job, worker_address);
                        }
                    }
                    None => unreachable!(),
                }
            }

            // For all the WriteStreams of this operator, let the Worker executing it
            // know the addresses of the destinations of this stream.
            for write_stream_id in &operator.write_streams {
                for destination in job_graph.destinations(write_stream_id) {
                    if let Some(worker_address) =
                        self.get_worker_address(worker_id, &driver_id, &destination, &placements)
                    {
                        worker_addresses.insert(destination, worker_address);
                    }
                }
            }

            // Inform the Worker to initiate appropriate connections and schedule
            // the Operator.
            tracing::trace!(
                "[Leader] Scheduling Job {:?} from Graph {:?} on Worker {} \
                    with the following addresses of other Workers: {:?}.",
                job,
                job_graph_id,
                worker_id,
                worker_addresses
            );
            let _ = leader_to_workers_tx.send(WorkerHandlerNotification::ScheduleJob(
                job_graph_id.clone(),
                *job,
                *worker_id,
                worker_addresses,
            ));
            job_status.insert(*job, (Some(*worker_id), ExecutionState::Scheduled));

            // Update the `WorkerState` to include the JobGraphID.
            self.worker_id_to_worker_state
                .get_mut(worker_id)
                .unwrap()
                .schedule_graph(job_graph_id.clone());
        }

        // Collect the addresses of the [`Worker`]s that retrieve or send
        // data on the [`IngressStream`] and the [`EgressStream`]s.
        let mut worker_addresses_for_driver = HashMap::new();
        for ingress_stream_id in job_graph.ingress_streams() {
            for destination in job_graph.destinations(&ingress_stream_id) {
                if let Some(worker_address) =
                    self.get_worker_address(&driver_id, &driver_id, &destination, &placements)
                {
                    worker_addresses_for_driver.insert(destination, worker_address);
                }
            }
        }
        for egress_stream_id in job_graph.egress_streams() {
            if let Some(job) = job_graph.source(&egress_stream_id) {
                if let Some(worker_address) =
                    self.get_worker_address(&driver_id, &driver_id, &job, &placements)
                {
                    worker_addresses_for_driver.insert(job, worker_address);
                }
            }
        }

        // Inform the Driver to initiate the appropriate connections, if there
        // were any [`IngressStream`]s or [`EgressStream`]s.
        if !worker_addresses_for_driver.is_empty() {
            let _ = leader_to_workers_tx.send(WorkerHandlerNotification::ScheduleJob(
                job_graph_id.clone(),
                Job::Driver,
                driver_id,
                worker_addresses_for_driver,
            ));
            job_status.insert(Job::Driver, (Some(driver_id), ExecutionState::Scheduled));

            // Update the `WorkerState` to include the JobGraphId.
            self.worker_id_to_worker_state
                .get_mut(&driver_id)
                .unwrap()
                .schedule_graph(job_graph_id.clone());
        }

        // Find all the jobs that were not scheduled from the Graph, and update the Metadata.
        for job in job_graph.operators().keys() {
            if !job_status.contains_key(job) {
                job_status.insert(*job, (None, ExecutionState::NotScheduled));
            }
        }
        self.job_graph_metadata.insert(
            job_graph_id.clone(),
            JobGraphMetadata::new(
                job_graph_id,
                job_graph,
                ExecutionState::Scheduled,
                job_status,
            ),
        );
    }

    /// Marks the [`Job`] ready in the [`AbstractJobGraph`] with the provided `job_graph_id`.
    ///
    /// If the specified `job` is the last Job of the graph that was left to be initialized,
    /// then ask the [`Worker`]s to execute the graph.
    ///
    /// # Arguments
    /// * `job_graph_id`: The ID of the [`AbstractJobGraph`] to which the `job` belongs.
    /// * `job`: The [`Job`] that was notified by the corresponding [`Worker`] to be ready.
    /// * `leader_to_workers_tx`: A channel to communicate the `ExecuteGraph` notification on
    ///    if the graph is ready for execution.
    fn mark_job_ready(
        &mut self,
        job_graph_id: JobGraphId,
        job: Job,
        leader_to_workers_tx: &broadcast::Sender<WorkerHandlerNotification>,
    ) {
        if let Some(graph_metadata) = self.job_graph_metadata.get_mut(&job_graph_id) {
            // Update the status of the `Job`.
            if !graph_metadata.update_job_state(&job, ExecutionState::Ready) {
                tracing::error!(
                    "[Leader] The Job {:?} was not found in the JobGraph {:?}.",
                    job,
                    job_graph_id
                );
                return;
            }

            // If all the Jobs are ready now, tell the Workers on which the graph
            // was scheduled to begin executing the JobGraph.
            if graph_metadata.all_jobs_in_state(ExecutionState::Ready) {
                let assigned_workers = graph_metadata.assigned_workers();
                tracing::debug!(
                    "[Leader] Notifying {:?} workers to execute the Graph {:?}.",
                    assigned_workers,
                    job_graph_id
                );
                match leader_to_workers_tx.send(WorkerHandlerNotification::ExecuteGraph(
                    job_graph_id.clone(),
                    assigned_workers,
                )) {
                    Ok(_) => {
                        // Transition all Jobs to the Execution state in the Metadata, and
                        // update the state of the JobGraph.
                        graph_metadata.update_all_job_states(ExecutionState::Executing);
                        graph_metadata.job_graph_state = ExecutionState::Executing;
                    }
                    Err(error) => {
                        tracing::error!(
                            "[Leader] Received error when requesting Workers to \
                                                execute JobGraph {:?}: {}",
                            job_graph_id,
                            error
                        );
                    }
                }
            } else {
                tracing::trace!(
                    "[Leader] The JobGraph {:?} was not ready for execution \
                                        after marking Job {:?} ready.",
                    job_graph_id.clone(),
                    job
                );
            }
        } else {
            tracing::error!(
                "[Leader] The JobGraph {:?} was not submitted to the Leader.",
                job_graph_id
            );
        }
    }
}
