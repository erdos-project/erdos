// TODO(Sukrit): Rename this to worker.rs once the merge is complete.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures::{stream::SplitSink, SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        control_plane::{
            notifications::{
                DriverNotification, LeaderNotification, QueryId, WorkerAddress, WorkerNotification,
            },
            ControlPlaneCodec,
        },
        data_plane::{
            notifications::{DataPlaneNotification, StreamType},
            DataPlane, StreamManager,
        },
        errors::CommunicationError,
    },
    dataflow::{
        graph::{AbstractStreamT, Job, JobGraph, JobGraphId},
        stream::StreamId,
    },
    node::{worker::Worker, Resources},
    Uuid,
};

use super::{operator_executors::OperatorExecutorT, ExecutionState, WorkerId};

/// An alias for the type of the connection between the [`Leader`] and the [`Worker`].
type ConnectionToLeader = SplitSink<
    Framed<TcpStream, ControlPlaneCodec<WorkerNotification, LeaderNotification>>,
    WorkerNotification,
>;

pub(crate) struct WorkerNode {
    /// The ID of the [`Worker`].
    id: WorkerId,
    /// The address of the [`Leader`] that the [`Worker`] will connect to.
    leader_address: SocketAddr,
    /// The address of the [`DataPlane`] where the [`Worker`] will listen
    /// for incoming connections from other [`Worker`]s.
    data_plane_address: SocketAddr,
    /// The set of [`Resources`] that the [`Worker`] owns.
    resources: Resources,
    /// A channel where the [`Worker`] receives notifications from the [`Driver`].
    channel_from_driver: UnboundedReceiver<DriverNotification>,
    /// A channel where the [`Worker`] sends notifications to the [`Driver`].
    channel_to_driver: UnboundedSender<DriverNotification>,
    /// A mapping of the [`JobGraph`]s that have been submitted to the [`Worker`].
    job_graphs: HashMap<JobGraphId, JobGraph>,
    /// A memo of the stream connections that are remaining to be setup for
    /// each [`Job`] before it can be marked Ready to the [`Leader`].
    pending_stream_setups: HashMap<Job, (JobGraphId, HashSet<StreamId>)>,
    /// A mapping of the `JobGraph` to the state of each scheduled `Job`.
    job_graph_to_job_state: HashMap<JobGraphId, HashMap<Job, ExecutionState>>,
    /// A memo of the OperatorExecutors that are pending for each JobGraph.
    job_graph_to_operator_executor: HashMap<JobGraphId, Vec<Box<dyn OperatorExecutorT>>>,
    /// A handle to the [`StreamManager`] instance shared with the [`DataPlane`].
    /// The [`DataPlane`] populates the channels on the shared instance upon request,
    /// which are then retrieved for consumption by each [`Job`].
    stream_manager: Arc<Mutex<StreamManager>>,
    /// A mapping of the pending Queries from the ID of the Query to its source.
    pending_queries: HashMap<QueryId, Job>,
    /// A data structure that executes the operators.
    // TODO (Sukrit): This should be incorporated into the WorkerNode.
    worker: Worker,
}

impl WorkerNode {
    /// Initializes a new [`Worker`] with the given ID and available [`Resources`].
    pub fn new(
        id: WorkerId,
        leader_address: SocketAddr,
        data_plane_address: SocketAddr,
        resources: Resources,
        channel_from_driver: UnboundedReceiver<DriverNotification>,
        channel_to_driver: UnboundedSender<DriverNotification>,
        num_threads: usize,
    ) -> Self {
        Self {
            id,
            leader_address,
            data_plane_address,
            resources,
            channel_from_driver,
            channel_to_driver,
            job_graphs: HashMap::new(),
            pending_stream_setups: HashMap::new(),
            job_graph_to_job_state: HashMap::new(),
            job_graph_to_operator_executor: HashMap::new(),
            stream_manager: Arc::new(Mutex::new(StreamManager::new(id))),
            pending_queries: HashMap::new(),
            worker: Worker::new(num_threads),
        }
    }

    /// Runs the main loop of the [`Worker`].
    /// A [`Worker`] connects to the [`Leader`], initiates a [`DataPlane`] for other [`Worker`]s
    /// to be able to connect to it, and then responds to notifications from the [`Leader`], the
    /// driver and other workers via the [`DataPlane`].
    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        // Connect to the Leader.
        tracing::trace!(
            "[Worker {}] Initializing Worker and connecting to Leader at address {}.",
            self.id,
            self.leader_address
        );
        let leader_connection = TcpStream::connect(self.leader_address).await?;
        let (mut leader_tx, mut leader_rx) = Framed::new(
            leader_connection,
            ControlPlaneCodec::<WorkerNotification, LeaderNotification>::default(),
        )
        .split();

        // Initialize the DataPlane on the specified address.
        tracing::trace!(
            "[Worker {}] Initiating a DataPlane for Worker at address {}.",
            self.id,
            self.data_plane_address
        );
        let (mut channel_to_data_plane_tx, channel_to_data_plane_rx) = mpsc::unbounded_channel();
        let (channel_from_data_plane_tx, mut channel_from_data_plane_rx) =
            mpsc::unbounded_channel();
        let mut data_plane = DataPlane::new(
            self.id,
            self.data_plane_address,
            Arc::clone(&self.stream_manager),
            channel_to_data_plane_rx,
            channel_from_data_plane_tx,
        )
        .await?;
        // The DataPlane might be required to bind to a randomly-assigned port,
        // so we retrieve the actual address and communicate it to the Leader.
        let data_plane_address = data_plane.address();
        let data_plane_handle = tokio::spawn(async move { data_plane.run().await });

        // Communicate the ID and DataPlane address of the Worker to the Leader.
        leader_tx
            .send(WorkerNotification::Initialized(
                self.id,
                data_plane_address,
                self.resources.clone(),
            ))
            .await?;
        tracing::debug!(
            "[Worker {}] Successfully Initialized Worker with the DataPlane address {}.",
            self.id,
            data_plane_address
        );

        // Respond to notifications from the Leader, the Driver and other Workers.
        loop {
            tokio::select! {
                // Handle messages received from the Leader.
                Some(msg_from_leader) = leader_rx.next() => {
                    match msg_from_leader {
                        Ok(msg_from_leader) => {
                            match msg_from_leader {
                                LeaderNotification::Shutdown => {
                                    tracing::info!(
                                        "[Worker {}] Shutting down upon request from the Leader.",
                                        self.id
                                    );
                                    return Ok(());
                                }
                                _ => {
                                    self.handle_leader_messages(
                                        msg_from_leader,
                                        &mut channel_to_data_plane_tx,
                                    ).await;
                                }
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                "[Worker {}] Received error when retrieving messages \
                                                            from the Leader: {:?}",
                                self.id,
                                error
                            );
                        },
                    }
                }

                // Handle messages received from the Driver.
                Some(driver_notification) = self.channel_from_driver.recv() => {
                    match driver_notification {
                        DriverNotification::Shutdown => {
                            tracing::info!(
                                "[Worker {}] Shutting down upon request from the Driver.",
                                self.id
                            );
                            if let Err(error) = leader_tx.send(WorkerNotification::Shutdown).await {
                                tracing::error!(
                                    "[Worker {}] Received an error when sending Shutdown message \
                                                                            to Leader: {:?}",
                                    self.id,
                                    error
                                );
                            }
                            tokio::join!(data_plane_handle);
                            return Ok(());
                        }
                        _ => self.handle_driver_messages(driver_notification, &mut leader_tx).await,
                    }
                }

                // Handle messages received from the DataPlane.
                Some(data_plane_notification) = channel_from_data_plane_rx.recv() => {
                    self.handle_data_plane_messages(data_plane_notification, &mut leader_tx).await;
                }
            }
        }
    }

    /// Responds to notifications received from the [`DataPlane`].
    async fn handle_data_plane_messages(
        &mut self,
        notification: DataPlaneNotification,
        leader_tx: &mut ConnectionToLeader,
    ) {
        match notification {
            DataPlaneNotification::StreamReady(job, stream_id) => {
                tracing::trace!(
                    "[Worker {}] Received StreamReady notification for Stream {} for Job {:?}.",
                    self.id,
                    stream_id,
                    job
                );

                // Remove the stream from the memo of streams left to finish setting
                // up for the given Job.
                match self.pending_stream_setups.get_mut(&job) {
                    Some((job_graph_id, pending_streams)) => {
                        match pending_streams.remove(&stream_id) {
                            true => {
                                // If the set is empty, notify the Leader of the
                                // successful initialization of the Job.
                                if pending_streams.is_empty() {
                                    // Initialize the Job.
                                    if let Some(job_graph) = self.job_graphs.get(job_graph_id) {
                                        if let Some(job_runner) = job_graph.job_runner(&job) {
                                            let stream_manager_copy =
                                                Arc::clone(&self.stream_manager);
                                            if let Some(operator_executor) =
                                                (job_runner)(stream_manager_copy)
                                            {
                                                let operator_executors = self
                                                    .job_graph_to_operator_executor
                                                    .entry(job_graph_id.clone())
                                                    .or_default();
                                                operator_executors.push(operator_executor);
                                            }
                                        } else {
                                            tracing::error!(
                                                "[Worker {}] Could not find a Runner for \
                                                Job {:?} in JobGraph {:?}.",
                                                self.id,
                                                job,
                                                job_graph_id
                                            );
                                        }
                                    } else {
                                        tracing::error!(
                                            "[Worker {}] Could not find a JobGraph with ID: {:?}.",
                                            self.id,
                                            job_graph_id,
                                        );
                                    }
                                    // TODO (Sukrit): The JobRunner should be invoked here.
                                    if let Err(error) = leader_tx
                                        .send(WorkerNotification::JobUpdate(
                                            job_graph_id.clone(),
                                            job,
                                            ExecutionState::Ready,
                                        ))
                                        .await
                                    {
                                        tracing::error!(
                                            "[Worker {}] Could not communicate the Ready status \
                                            of Job {:?} from the JobGraph {:?} to the Leader. \
                                                                    Received error {:?}",
                                            self.id,
                                            job,
                                            job_graph_id,
                                            error,
                                        );
                                    }

                                    // Change the state of the Job in the JobGraph.
                                    match self.job_graph_to_job_state.get_mut(job_graph_id) {
                                        Some(job_state) => match job_state.get_mut(&job) {
                                            Some(job_state) => {
                                                *job_state = ExecutionState::Ready;
                                            }
                                            None => {
                                                tracing::warn!(
                                                    "[Worker {}] Could not find the state of \
                                                            the Job {:?} that was supposed to be \
                                                            scheduled for the JobGraph {:?}.",
                                                    self.id,
                                                    job,
                                                    job_graph_id,
                                                )
                                            }
                                        },
                                        None => {
                                            tracing::warn!(
                                                "[Worker {}] Inconsistency between the state of \
                                                the pending streams for Job {:?} and the state \
                                                    of the JobGraph {:?} to which it belongs.",
                                                self.id,
                                                job,
                                                job_graph_id
                                            );
                                        }
                                    }

                                    // Remove the mapping from the pending setups.
                                    self.pending_stream_setups.remove(&job);
                                }
                            }
                            false => {
                                tracing::warn!(
                                    "[Worker {}] Could not find pending Stream {:?} for \
                                                Job {:?} from the JobGraph {:?}.",
                                    self.id,
                                    stream_id,
                                    job,
                                    job_graph_id,
                                );
                            }
                        }
                    }
                    None => {
                        tracing::warn!(
                            "[Worker {}] Inconsistency between the state of \
                                the pending Stream setups for Job {:?}.",
                            self.id,
                            job,
                        );
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    /// Responds to notifications received from the [`Leader`].
    async fn handle_leader_messages(
        &mut self,
        msg_from_leader: LeaderNotification,
        channel_to_data_plane: &mut UnboundedSender<DataPlaneNotification>,
    ) {
        match msg_from_leader {
            LeaderNotification::ScheduleJob(job_graph_id, job, worker_addresses) => {
                self.schedule_job(job_graph_id, job, &worker_addresses, channel_to_data_plane);
            }
            LeaderNotification::ExecuteGraph(job_graph_id) => {
                tracing::debug!(
                    "[Worker {}] Executing JobGraph {:?}.",
                    self.id,
                    job_graph_id
                );
                tracing::info!(
                    "[Worker {}] The state of the JobGraph is {:?}.",
                    self.id,
                    self.job_graph_to_job_state
                );

                if let Some(operator_executors) =
                    self.job_graph_to_operator_executor.remove(&job_graph_id)
                {
                    self.worker.spawn_tasks(operator_executors).await;
                    self.worker.execute().await;
                } else {
                    tracing::error!(
                        "[Worker {}] Could not find any executors for JobGraph {:?}.
                            The graph was potentially not scheduled here.",
                        self.id,
                        job_graph_id,
                    );
                }
            }
            LeaderNotification::QueryResponse(query_id, query_response) => {
                if let Some(source) = self.pending_queries.get(&query_id) {
                    match source {
                        Job::Driver => todo!(),
                        // Queries are only initiated by the Driver for now.
                        Job::Operator(_) => unreachable!(),
                    }
                } else {
                    tracing::warn!(
                        "[Worker {}] Received a response to the Query {:?} \
                                    that was not pending on the Worker.",
                        self.id,
                        query_id,
                    );
                }
            }
            // The shutdown arm is unreachable, because it should be handled in the main loop.
            LeaderNotification::Shutdown => unreachable!(),
        }
    }

    /// Responds to the notifications received from the Driver.
    async fn handle_driver_messages(
        &mut self,
        driver_notification: DriverNotification,
        leader_tx: &mut ConnectionToLeader,
    ) {
        match driver_notification {
            DriverNotification::RegisterGraph(job_graph) => {
                // Save the JobGraph.
                let job_graph_id = job_graph.id();
                tracing::debug!(
                    "[Worker {}] Registered the JobGraph {:?}.",
                    self.id,
                    job_graph_id
                );
                self.job_graphs.insert(job_graph_id, job_graph);
            }
            DriverNotification::SubmitGraph(job_graph_id) => {
                // Retrieve the JobGraph and communicate an Abstract version
                // of the graph to the Leader.
                if let Some(job_graph) = self.job_graphs.get(&job_graph_id) {
                    let internal_graph = job_graph.clone().into();
                    if let Err(error) = leader_tx
                        .send(WorkerNotification::SubmitGraph(
                            job_graph_id,
                            internal_graph,
                        ))
                        .await
                    {
                        tracing::error!(
                            "[Worker {}] Received an error when sending Abstract \
                                                Graph message to Leader: {:?}",
                            self.id,
                            error
                        );
                    };
                } else {
                    tracing::error!(
                        "[Worker {}] Found no JobGraph with ID {:?}.",
                        self.id,
                        job_graph_id,
                    )
                }
            }
            DriverNotification::Query(query) => {
                // Generate a QueryId and forward the Query to the Leader.
                let query_id = QueryId(Uuid::new_deterministic());
                if let Err(error) = leader_tx
                    .send(WorkerNotification::Query(query_id.clone(), query.clone()))
                    .await
                {
                    tracing::error!(
                        "[Worker {}] Received an error when sending the \
                                    Query {:?} to the Leader: {:?}",
                        self.id,
                        query,
                        error,
                    );
                } else {
                    // Save the pending Query corresponding to the Driver job.
                    self.pending_queries.insert(query_id, Job::Driver);
                }
            }
            // The shutdown arm is unreachable, because it should be handled in the main loop.
            DriverNotification::Shutdown => unreachable!(),
        }
    }

    fn schedule_job(
        &mut self,
        job_graph_id: JobGraphId,
        job: Job,
        worker_addresses: &HashMap<Job, WorkerAddress>,
        channel_to_data_plane: &mut UnboundedSender<DataPlaneNotification>,
    ) {
        let job_graph = match self.job_graphs.get(&job_graph_id) {
            Some(job_graph) => job_graph,
            None => {
                tracing::error!(
                    "[Worker {}] JobGraph {:?} was not registered on this Worker.",
                    self.id,
                    job_graph_id,
                );
                return;
            }
        };

        // Construct the Streams to setup for the scheduled Job.
        let mut streams_to_setup = Vec::new();
        match job {
            Job::Operator(_) => {
                if let Some(operator) = job_graph.operator(&job) {
                    let operator_name = match &operator.config.name {
                        Some(name) => name.clone(),
                        None => "UnnamedOperator".to_string(),
                    };
                    tracing::debug!(
                        "[Worker {}] Scheduling Operator {} (ID={:?}) from JobGraph {:?}.",
                        self.id,
                        operator_name,
                        operator.id,
                        job_graph.id(),
                    );

                    // Request the DataPlane to setup the WriteStreams.
                    streams_to_setup.extend(operator.write_streams.iter().filter_map(
                        |stream_id| {
                            let stream = job_graph.stream(stream_id)?;
                            let worker_addresses =
                                self.get_write_stream_addresses(&stream, worker_addresses);
                            Some(StreamType::WriteStream(stream, worker_addresses))
                        },
                    ));

                    // Request the DataPlane to setup the ReadStreams.
                    streams_to_setup.extend(operator.read_streams.iter().filter_map(|stream_id| {
                        let stream = job_graph.stream(stream_id)?;
                        let worker_addresses =
                            self.get_read_stream_address(&stream, worker_addresses)?;
                        Some(StreamType::ReadStream(stream, worker_addresses))
                    }));
                } else {
                    tracing::error!(
                        "[Worker {}] The Job {:?} was not found in JobGraph {:?}.",
                        self.id,
                        job,
                        job_graph.id(),
                    );
                }
            }
            Job::Driver => {
                // Request the DataPlane to setup the IngressStreams.
                streams_to_setup.extend(job_graph.ingress_streams().into_iter().map(|stream| {
                    let worker_addresses =
                        self.get_write_stream_addresses(&stream, worker_addresses);
                    StreamType::IngressStream(stream, worker_addresses)
                }));

                // Request the DataPlane to setup the EgressStreams.
                streams_to_setup.extend(job_graph.egress_streams().into_iter().filter_map(
                    |stream| {
                        let worker_addresses =
                            self.get_read_stream_address(&stream, worker_addresses)?;
                        Some(StreamType::EgressStream(stream, worker_addresses))
                    },
                ));
            }
        }

        // Cache the streams that need to be initialized to call this Job ready.
        let pending_setups = streams_to_setup.iter().map(|stream| stream.id()).collect();
        tracing::trace!(
            "[Worker {}] The Job {:?} is pending setup of {:?} streams.",
            self.id,
            job,
            pending_setups
        );
        self.pending_stream_setups
            .insert(job, (job_graph.id(), pending_setups));

        // Add the Job to the set of scheduled Jobs for this JobGraph.
        let job_state = self
            .job_graph_to_job_state
            .entry(job_graph.id())
            .or_default();
        job_state.insert(job, ExecutionState::Scheduled);

        // Ask the DataPlane to setup the Streams.
        if let Err(error) =
            channel_to_data_plane.send(DataPlaneNotification::SetupStreams(job, streams_to_setup))
        {
            tracing::warn!(
                "[Worker {}] Received error when requesting the setup of \
                                                                streams for Job {:?}: {:?}",
                self.id,
                job,
                error,
            )
        }
    }

    fn get_read_stream_address(
        &self,
        stream: &Box<dyn AbstractStreamT>,
        worker_addresses: &HashMap<Job, WorkerAddress>,
    ) -> Option<WorkerAddress> {
        let source_job = stream.source()?;
        match worker_addresses.get(&source_job) {
            Some(source_address) => Some(source_address.clone()),
            None => {
                tracing::warn!(
                    "[Worker {}] Could not find address of the source Job {:?} for
                    the Stream {} (ID={}) in the addresses provided by the Leader.",
                    self.id,
                    source_job,
                    stream.name(),
                    stream.id(),
                );
                None
            }
        }
    }

    fn get_write_stream_addresses(
        &self,
        stream: &Box<dyn AbstractStreamT>,
        worker_addresses: &HashMap<Job, WorkerAddress>,
    ) -> HashMap<Job, WorkerAddress> {
        let mut destination_addresses = HashMap::new();
        for destination_job in stream.destinations() {
            match worker_addresses.get(&destination_job) {
                Some(destination_address) => {
                    destination_addresses.insert(destination_job, destination_address.clone());
                }
                None => {
                    tracing::warn!(
                        "[Worker {}] Could not find address of the destination Job {:?} \
                        for the Stream {} (ID={}) in the addresses provided by the Leader.",
                        self.id,
                        destination_job,
                        stream.name(),
                        stream.id(),
                    );
                }
            }
        }
        destination_addresses
    }

    pub(crate) fn id(&self) -> WorkerId {
        self.id.clone()
    }
}
