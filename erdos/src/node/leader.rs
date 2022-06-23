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
        CommunicationError,
    },
    dataflow::{
        graph::{InternalGraph, Job},
        stream::StreamId,
    },
    node::WorkerState,
    scheduler::{JobGraphScheduler, SimpleJobGraphScheduler},
    OperatorId,
};

use super::worker::Worker;

/// The [`InterThreadMessage`] enum defines the messages that the different
/// spawned tasks may communicate back to the main loop of the [`LeaderNode`].
#[derive(Debug, Clone)]
enum InterThreadMessage {
    WorkerInitialized(WorkerState),
    ScheduleJobGraph(String, InternalGraph),
    ScheduleOperator(String, OperatorId, usize, HashMap<StreamId, WorkerAddress>),
    OperatorReady(String, OperatorId),
    ExecuteGraph(String),
    Shutdown(usize),
    ShutdownAllWorkers,
}

#[derive(PartialEq)]
enum OperatorState {
    Ready,
    NotReady,
}

pub(crate) struct LeaderNode {
    /// The address that the LeaderNode binds to.
    address: SocketAddr,
    /// A Receiver corresponding to a channel between the Driver and the Leader.
    driver_notification_rx: Receiver<DriverNotification>,
    /// A Vector containing the handlers corresponding to each Worker.
    worker_handlers: Vec<JoinHandle<()>>,
    /// A mapping between the ID of the Worker and the state maintained for it.
    worker_id_to_worker_state: HashMap<usize, WorkerState>,
    /// The scheduler to be used for scheduling JobGraphs onto Workers.
    job_graph_scheduler: Box<dyn JobGraphScheduler + Send>,
    /// A mapping between the name of the Job and the status of its Operators.
    job_to_operator_state_map: HashMap<String, HashMap<OperatorId, OperatorState>>,
}

impl LeaderNode {
    pub fn new(address: SocketAddr, driver_notification_rx: Receiver<DriverNotification>) -> Self {
        Self {
            address,
            driver_notification_rx,
            worker_handlers: Vec::new(),
            worker_id_to_worker_state: HashMap::new(),
            // TODO (Sukrit): The type of Scheduler should be chosen by a Configuration.
            job_graph_scheduler: Box::new(SimpleJobGraphScheduler::default()),
            job_to_operator_state_map: HashMap::new(),
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
                            let worker_handler = tokio::spawn(LeaderNode::handle_worker(
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
                    .insert(worker_state.get_id(), worker_state);
            }
            InterThreadMessage::ScheduleJobGraph(job_name, job_graph) => {
                // Invoke the Scheduler to retrieve the placements for this JobGraph.
                let workers = self.worker_id_to_worker_state.values().cloned().collect();
                let placements = self
                    .job_graph_scheduler
                    .schedule_graph(&job_graph, &workers);

                // Broadcast the ScheduleOperator message for the placed operators.
                // Maintain a flag for each operator that checks if the operator has
                // been initialized or not.
                let mut operator_status = HashMap::new();
                for (operator_id, worker_id) in placements.iter() {
                    // For all the sources of the Streams for this operator, let the Worker
                    // know the address of the Workers executing them.
                    let operator = job_graph.get_operator(operator_id).unwrap();
                    let mut source_worker_addresses = HashMap::new();
                    for read_stream_id in &operator.read_streams {
                        match job_graph.get_source_operator(read_stream_id) {
                            Some(job) => {
                                match job {
                                    Job::Operator(source_operator_id) => {
                                        let source_worker_id =
                                            placements.get(&source_operator_id).unwrap();
                                        let source_address = if source_worker_id == worker_id {
                                            WorkerAddress::Local
                                        } else {
                                            // If the Source of the Stream is not on the same
                                            // Worker, then communicate the address of the Source.
                                            let source_worker_address = self
                                                .worker_id_to_worker_state
                                                .get(source_worker_id)
                                                .unwrap()
                                                .get_address();
                                            WorkerAddress::Remote(
                                                *source_worker_id,
                                                source_worker_address,
                                            )
                                        };
                                        source_worker_addresses
                                            .insert(*read_stream_id, source_address);
                                    }
                                    Job::Driver => todo!(),
                                }
                            }
                            None => unreachable!(),
                        }
                    }
                    let _ = leader_to_workers_tx.send(InterThreadMessage::ScheduleOperator(
                        job_name.clone(),
                        operator_id.clone(),
                        *worker_id,
                        source_worker_addresses,
                    ));
                    operator_status.insert(operator_id.clone(), OperatorState::NotReady);
                }

                // Map the flags that check if the Operator is ready to the name of the JobGraph.
                self.job_to_operator_state_map
                    .insert(job_name, operator_status);
            }
            InterThreadMessage::OperatorReady(job_name, operator_id) => {
                // Change the status of the Operator for the Job.
                let mut job_ready_to_execute = false;
                if let Some(operator_status) = self.job_to_operator_state_map.get_mut(&job_name) {
                    if let Some(operator_status_value) = operator_status.get_mut(&operator_id) {
                        *operator_status_value = OperatorState::Ready;
                    } else {
                        tracing::error!(
                            "The Operator {} was not found in the Job {}.",
                            operator_id,
                            job_name
                        );
                    }

                    // If all the Operators are ready now, tell the Workers to
                    // begin executing the JobGraph.
                    if operator_status
                        .values()
                        .into_iter()
                        .all(|status| *status == OperatorState::Ready)
                    {
                        let _ = leader_to_workers_tx
                            .send(InterThreadMessage::ExecuteGraph(job_name.clone()));
                        job_ready_to_execute = true;
                    }
                } else {
                    tracing::error!("The Job {} was not submitted to the Leader.", job_name);
                }

                // If the Job was executed, remove the state from the Map.
                if job_ready_to_execute {
                    self.job_to_operator_state_map.remove(&job_name);
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

        let mut id_of_this_worker = 0;

        // Handle messages from the Worker and the Leader.
        loop {
            tokio::select! {
                // Communicate messages received from the Worker to the Leader.
                Some(Ok(msg_from_worker)) = worker_rx.next() => {
                    match msg_from_worker {
                        WorkerNotification::Initialized(worker_state) => {
                            id_of_this_worker = worker_state.get_id();
                            // Communicate the Worker ID to the Leader.
                            tracing::debug!(
                                "Initialized a Worker with the ID {} at {}",
                                worker_state.get_id(),
                                worker_state.get_address(),
                            );
                            let _ = channel_to_leader.send(
                                InterThreadMessage::WorkerInitialized(worker_state)
                            );
                        },
                        WorkerNotification::OperatorReady(job_name, operator_id) => {
                            tracing::trace!(
                                "Operator {} from Job {} is ready on Worker {}.",
                                operator_id,
                                job_name,
                                id_of_this_worker
                            );
                            let _ = channel_to_leader.send(
                                InterThreadMessage::OperatorReady(job_name, operator_id)
                            );
                        }
                        WorkerNotification::SubmitGraph(job_name, job_graph) => {
                            tracing::trace!(
                                "Leader received graph from Worker with ID: {}.",
                                id_of_this_worker
                            );
                            let _ = channel_to_leader.send(
                                InterThreadMessage::ScheduleJobGraph(job_name, job_graph)
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
                        InterThreadMessage::ScheduleOperator(
                            job_name,
                            operator_id,
                            worker_id,
                            source_worker_addresses,
                        ) => {
                            // The Leader assigns an operator to a worker.
                            if id_of_this_worker == worker_id {
                                tracing::debug!(
                                    "The Operator with ID {} was scheduled on {}.",
                                    operator_id,
                                    worker_id,
                                );
                                let _ = worker_tx
                                    .send(LeaderNotification::ScheduleOperator(
                                        job_name,
                                        operator_id,
                                        source_worker_addresses,
                                    ))
                                    .await;
                            }
                        }
                        InterThreadMessage::ExecuteGraph(job_name) => {
                            // Tell the Worker to execute the operators for this graph.
                            let _ = worker_tx
                                .send(LeaderNotification::ExecuteGraph(job_name.clone()))
                                .await;
                            tracing::debug!(
                                "The JobGraph {} is ready to execute on Worker {}",
                                job_name,
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
}
