use std::{collections::HashMap, net::SocketAddr};

use futures::{future, SinkExt, StreamExt};
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
        control_plane::{
            notifications::{
                DriverNotification, LeaderNotification, WorkerAddress, WorkerNotification,
            },
            ControlPlaneCodec,
        },
        errors::CommunicationError,
    },
    dataflow::graph::{AbstractJobGraph, Job, JobGraphId},
    node::WorkerState,
    scheduler::{JobGraphScheduler, SimpleJobGraphScheduler},
};

use super::WorkerId;

/// The [`InterThreadMessage`] enum defines the messages that the different
/// spawned tasks may communicate back to the main loop of the [`LeaderNode`].
#[derive(Debug, Clone)]
enum InterThreadMessage {
    WorkerInitialized(WorkerState),
    ScheduleJobGraph(WorkerId, JobGraphId, AbstractJobGraph),
    ScheduleJob(JobGraphId, Job, WorkerId, HashMap<Job, WorkerAddress>),
    JobReady(JobGraphId, Job),
    // TODO (Sukrit): Bookkeep that if the graph was not placed on a Worker,
    // then it should not be asked to be executed on that worker.
    ExecuteGraph(JobGraphId),
    Shutdown(WorkerId),
    ShutdownAllWorkers,
}

#[derive(PartialEq)]
enum JobState {
    Ready,
    NotReady,
}

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
    /// A mapping between the JobGraph and the status of its Jobs.
    job_graph_to_job_state: HashMap<JobGraphId, HashMap<Job, JobState>>,
}

impl Leader {
    pub fn new(address: SocketAddr, driver_notification_rx: Receiver<DriverNotification>) -> Self {
        Self {
            address,
            driver_notification_rx,
            worker_handlers: Vec::new(),
            worker_id_to_worker_state: HashMap::new(),
            // TODO (Sukrit): The type of Scheduler should be chosen by a Configuration.
            job_graph_scheduler: Box::new(SimpleJobGraphScheduler::default()),
            job_graph_to_job_state: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
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
                                "Leader received a Worker connection from address: {}",
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
                                "Leader received an error when handling a Worker connection: {}",
                                error
                            );
                        }
                    }
                }

                // Handle new messages from the drivers.
                Some(driver_notification) = self.driver_notification_rx.recv() => {
                    match driver_notification {
                        DriverNotification::Shutdown => {
                            // Ask all Workers to shutdown.
                            tracing::trace!(
                                "Leader received a Shutdown notification from the driver. \
                                Requesting all the Workers to shutdown."
                            );
                            if let Err(error) =
                                leader_to_workers_tx.send(InterThreadMessage::ShutdownAllWorkers) {
                                tracing::error!(
                                    "Leader received an error when requesting Worker shutdown: {}",
                                    error
                                );
                            }

                            // Wait for all worker handler tasks to shutdown.
                            future::join_all(self.worker_handlers.drain(..)).await;
                            tracing::info!("Leader is shutting down!");
                            return Ok(());
                        }
                        _ => {}
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
    /// threads running the [`handle_worker`] method for each [`Worker`].
    fn handle_worker_message(
        &mut self,
        worker_handler_msg: InterThreadMessage,
        leader_to_workers_tx: &broadcast::Sender<InterThreadMessage>,
    ) {
        match worker_handler_msg {
            InterThreadMessage::WorkerInitialized(worker_state) => {
                self.worker_id_to_worker_state
                    .insert(worker_state.id(), worker_state);
            }
            InterThreadMessage::ScheduleJobGraph(driver_id, job_graph_id, job_graph) => {
                // We consider the Worker scheduling the JobGraph to be the Driver.
                // Invoke the Scheduler to retrieve the placements for this JobGraph.
                let workers = self.worker_id_to_worker_state.values().cloned().collect();
                let placements = self
                    .job_graph_scheduler
                    .schedule_graph(&job_graph, &workers);

                // Maintain a flag for each operator that checks if the operator has
                // been initialized or not.
                let mut job_status = HashMap::new();

                // Broadcast the ScheduleOperator message for the placed operators.
                for (job, worker_id) in placements.iter() {
                    let operator = job_graph.operator(job).unwrap();
                    let mut worker_addresses = HashMap::new();

                    // For all the ReadStreams of this operator, let the Worker executing it
                    // know the addresses of the source operator of the stream.
                    for read_stream_id in &operator.read_streams {
                        match job_graph.source(read_stream_id) {
                            Some(job) => {
                                if let Some(worker_address) = self.get_worker_address(
                                    *worker_id,
                                    driver_id,
                                    &job,
                                    &placements,
                                ) {
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
                            if let Some(worker_address) = self.get_worker_address(
                                *worker_id,
                                driver_id,
                                &destination,
                                &placements,
                            ) {
                                worker_addresses.insert(destination, worker_address);
                            }
                        }
                    }

                    // Inform the Worker to initiate appropriate connections and schedule
                    // the Operator.
                    let _ = leader_to_workers_tx.send(InterThreadMessage::ScheduleJob(
                        job_graph_id.clone(),
                        job.clone(),
                        *worker_id,
                        worker_addresses,
                    ));
                    job_status.insert(job.clone(), JobState::NotReady);
                }

                // Collect the addresses of the [`Worker`]s that retrieve or send
                // data on the [`IngressStream`] and the [`EgressStream`]s.
                let mut worker_addresses_for_driver = HashMap::new();
                for ingress_stream_id in job_graph.ingress_streams() {
                    for destination in job_graph.destinations(&ingress_stream_id) {
                        if let Some(worker_address) =
                            self.get_worker_address(driver_id, driver_id, &destination, &placements)
                        {
                            worker_addresses_for_driver.insert(destination, worker_address);
                        }
                    }
                }
                for egress_stream_id in job_graph.egress_streams() {
                    if let Some(job) = job_graph.source(&egress_stream_id) {
                        if let Some(worker_address) =
                            self.get_worker_address(driver_id, driver_id, &job, &placements)
                        {
                            worker_addresses_for_driver.insert(job, worker_address);
                        }
                    }
                }

                // Inform the Driver to initiate the appropriate connections, if there
                // were any [`IngressStream`]s or [`EgressStream`]s.
                if !worker_addresses_for_driver.is_empty() {
                    let _ = leader_to_workers_tx.send(InterThreadMessage::ScheduleJob(
                        job_graph_id.clone(),
                        Job::Driver,
                        driver_id,
                        worker_addresses_for_driver,
                    ));
                    job_status.insert(Job::Driver, JobState::NotReady);
                }

                // Map the flags that check if the Operator is ready to the name of the JobGraph.
                self.job_graph_to_job_state.insert(job_graph_id, job_status);
            }
            InterThreadMessage::JobReady(job_graph_id, job) => {
                // Change the status of the Job for this JobGraph.
                let mut job_graph_ready_to_execute = false;
                if let Some(job_status) = self.job_graph_to_job_state.get_mut(&job_graph_id) {
                    if let Some(job_status_value) = job_status.get_mut(&job) {
                        *job_status_value = JobState::Ready;
                    } else {
                        tracing::error!(
                            "The Job {:?} was not found in the JobGraph {:?}.",
                            job,
                            job_graph_id
                        );
                    }

                    // If all the Operators are ready now, tell the Workers to
                    // begin executing the JobGraph.
                    if job_status
                        .values()
                        .into_iter()
                        .all(|status| *status == JobState::Ready)
                    {
                        let _ = leader_to_workers_tx
                            .send(InterThreadMessage::ExecuteGraph(job_graph_id.clone()));
                        job_graph_ready_to_execute = true;
                    }
                } else {
                    tracing::error!(
                        "The JobGraph {:?} was not submitted to the Leader.",
                        job_graph_id
                    );
                }

                // If the Job was executed, remove the state from the Map.
                if job_graph_ready_to_execute {
                    self.job_graph_to_job_state.remove(&job_graph_id);
                }
            }
            InterThreadMessage::Shutdown(worker_id) => {
                self.worker_id_to_worker_state.remove(&worker_id);
            }
            _ => {
                todo!();
            }
        }
    }

    async fn handle_worker(
        worker_stream: TcpStream,
        channel_to_leader: UnboundedSender<InterThreadMessage>,
        mut channel_from_leader: broadcast::Receiver<InterThreadMessage>,
    ) {
        let (mut worker_tx, mut worker_rx) = Framed::new(
            worker_stream,
            ControlPlaneCodec::<LeaderNotification, WorkerNotification>::default(),
        )
        .split();

        let mut id_of_this_worker = WorkerId::default();

        // Handle messages from the Worker and the Leader.
        loop {
            tokio::select! {
                // Communicate messages received from the Worker to the Leader.
                Some(Ok(msg_from_worker)) = worker_rx.next() => {
                    match msg_from_worker {
                        WorkerNotification::Initialized(worker_state) => {
                            id_of_this_worker = worker_state.id();
                            // Communicate the Worker ID to the Leader.
                            tracing::debug!(
                                "Initialized a Worker with the ID {} at {}",
                                worker_state.id(),
                                worker_state.address(),
                            );
                            let _ = channel_to_leader.send(
                                InterThreadMessage::WorkerInitialized(worker_state)
                            );
                        },
                        WorkerNotification::JobReady(job_graph_id, job) => {
                            tracing::trace!(
                                "Job {:?} from JobGraph {:?} is ready on Worker {}.",
                                job,
                                job_graph_id,
                                id_of_this_worker
                            );
                            let _ = channel_to_leader.send(
                                InterThreadMessage::JobReady(job_graph_id, job)
                            );
                        }
                        WorkerNotification::SubmitGraph(job_graph_id, job_graph) => {
                            tracing::trace!(
                                "Leader received graph from Worker with ID: {}.",
                                id_of_this_worker
                            );
                            let _ = channel_to_leader.send(
                                InterThreadMessage::ScheduleJobGraph(id_of_this_worker, job_graph_id, job_graph)
                            );
                        }
                        WorkerNotification::Shutdown => {
                            tracing::info!(
                                "Worker {} is shutting down.",
                                id_of_this_worker
                            );
                            let _ = channel_to_leader.send(
                                InterThreadMessage::Shutdown(id_of_this_worker)
                            );
                            return;
                        }
                    }
               }

                // Communicate messages received from the Leader to the Worker.
                Ok(msg_from_leader) = channel_from_leader.recv() => {
                    match msg_from_leader {
                        InterThreadMessage::ScheduleJob(
                            job_graph_id,
                            job,
                            worker_id,
                            worker_addresses,
                        ) => {
                            // The Leader assigns an operator to a worker.
                            if id_of_this_worker == worker_id {
                                tracing::debug!(
                                    "The Job {:?} from JobGraph {:?} was scheduled on {}.",
                                    job,
                                    job_graph_id,
                                    worker_id,
                                );
                                let _ = worker_tx
                                    .send(LeaderNotification::ScheduleJob(
                                        job_graph_id,
                                        job,
                                        worker_addresses,
                                    ))
                                    .await;
                            }
                        }
                        InterThreadMessage::ExecuteGraph(job_graph_id) => {
                            // Tell the Worker to execute the operators for this graph.
                            let _ = worker_tx
                                .send(LeaderNotification::ExecuteGraph(job_graph_id.clone()))
                                .await;
                            tracing::debug!(
                                "The JobGraph {:?} is ready to execute on Worker {}",
                                job_graph_id,
                                id_of_this_worker,
                            );
                        }
                        InterThreadMessage::ShutdownAllWorkers => {
                            // The Leader requested all nodes to shutdown.
                            let _ = worker_tx.send(LeaderNotification::Shutdown).await;
                            tracing::debug!(
                                "The handler for Worker {} is shutting down.",
                                id_of_this_worker
                            );
                            return;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn get_worker_address(
        &self,
        worker_id: WorkerId,
        driver_id: WorkerId,
        job: &Job,
        placements: &HashMap<Job, WorkerId>,
    ) -> Option<WorkerAddress> {
        let worker_address = match job {
            Job::Operator(_) => {
                let worker_id_for_operator = *placements.get(job)?;
                if worker_id_for_operator == worker_id {
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
            Job::Driver => {
                if driver_id == worker_id {
                    // NOTE: This should never happen since no jobs should be placed by
                    // the Scheduler on the Driver, but we let this remain to ensure that
                    // simplistic setups with the placements pre-specified using
                    // Configurations still work.
                    WorkerAddress::Local
                } else {
                    let driver_address = self.worker_id_to_worker_state.get(&driver_id)?.address();
                    WorkerAddress::Remote(driver_id, driver_address)
                }
            }
        };
        Some(worker_address)
    }
}
