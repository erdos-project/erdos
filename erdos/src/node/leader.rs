use std::{collections::HashMap, net::SocketAddr};

use futures::{future, SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        broadcast::{self, Receiver},
        mpsc::{self, UnboundedSender},
    },
    task::JoinHandle,
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        CommunicationError, ControlPlaneCodec, DriverNotification, LeaderNotification, WorkerId,
        WorkerNotification,
    },
    node::Resources,
    OperatorId,
};

/// The [`InterThreadMessage`] enum defines the messages that the different
/// spawned tasks may communicate back to the main loop of the [`LeaderNode`].
#[derive(Debug, Clone)]
enum InterThreadMessage {
    WorkerInitialized(WorkerId, Resources),
    ScheduleOperator(OperatorId, WorkerId),
    Shutdown(WorkerId),
    ShutdownAllWorkers,
}

#[derive(Debug)]
struct WorkerState {
    id: WorkerId,
    resources: Resources,
}

impl WorkerState {
    fn new(id: WorkerId, resources: Resources) -> Self {
        Self { id, resources }
    }
}

pub struct LeaderNode {
    /// The address that the LeaderNode binds to.
    address: SocketAddr,
    /// A Receiver corresponding to a channel between the Driver and the Leader.
    driver_notification_rx: Receiver<DriverNotification>,
    /// A Vector containing the handlers corresponding to each Worker.
    worker_handlers: Vec<JoinHandle<()>>,
    /// A mapping between the ID of the Worker and the state maintained for it.
    worker_id_to_worker_state: HashMap<WorkerId, WorkerState>,
}

impl LeaderNode {
    pub fn new(address: SocketAddr, driver_notification_rx: Receiver<DriverNotification>) -> Self {
        Self {
            address,
            driver_notification_rx,
            worker_handlers: Vec::new(),
            worker_id_to_worker_state: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        let leader_listener = TcpListener::bind(self.address).await?;
        let (workers_to_leader_tx, mut workers_to_leader_rx) = mpsc::unbounded_channel();
        let (leader_to_workers_tx, _) = broadcast::channel(100);

        loop {
            tokio::select! {
               // Handle new Worker connections.
                listener_result = leader_listener.accept() => {
                    match listener_result {
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
                driver_notification = self.driver_notification_rx.recv() => {
                    match driver_notification {
                        Ok(DriverNotification::Shutdown) => {
                            // Ask all Workers to shutdown.
                            tracing::trace!(
                                "Leader received a Shutdown notification from the driver.\
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
                        Err(error) => {
                            // TODO (Sukrit) :: What should happen when the connection to
                            // the driver is lost?
                            tracing::error!(
                                "Leader received an error from the driver: {}",
                                error
                            );
                        },
                    }
                }

                // Handle new messages from the Worker handlers.
                Some(worker_handler_msg) = workers_to_leader_rx.recv() =>  {
                    match worker_handler_msg {
                        InterThreadMessage::WorkerInitialized(worker_id, worker_resources) => {
                            self.worker_id_to_worker_state.insert(
                                worker_id,
                                WorkerState::new(worker_id, worker_resources)
                            );
                        }
                        InterThreadMessage::Shutdown(worker_id) => {
                            self.worker_id_to_worker_state.remove(&worker_id);
                        }
                        _ => {todo!();}
                    }
                }
            }
        }
    }

    async fn handle_worker(
        worker_stream: TcpStream,
        channel_to_leader: UnboundedSender<InterThreadMessage>,
        mut channel_from_leader: Receiver<InterThreadMessage>,
    ) {
        let (mut worker_tx, mut worker_rx) = Framed::new(
            worker_stream,
            ControlPlaneCodec::<LeaderNotification, WorkerNotification>::default(),
        )
        .split();

        let mut id_of_this_worker: WorkerId = WorkerId::nil();

        // Handle messages from the Worker and the Leader.
        loop {
            tokio::select! {
                // Communicate messages received from the Worker to the Leader.
                Some(Ok(msg_from_worker)) = worker_rx.next() => {
                    match msg_from_worker {
                        WorkerNotification::Initialized(worker_id, worker_resources) => {
                            id_of_this_worker = worker_id;
                            // Communicate the Worker ID to the Leader.
                            tracing::debug!(
                                "Initialized a Worker with the ID: {}",
                                worker_id
                            );
                            let _ = channel_to_leader.send(
                                InterThreadMessage::WorkerInitialized(worker_id, worker_resources)
                            );
                        },
                        WorkerNotification::OperatorReady(operator_id) => {
                            tracing::trace!(
                                "Operator {} is ready on Worker {}.",
                                operator_id,
                                id_of_this_worker
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
                        InterThreadMessage::ScheduleOperator(operator_id, worker_id) => {
                            // The Leader assigns an operator to a worker.
                            if id_of_this_worker == worker_id {
                                let _ = worker_tx.send(
                                    LeaderNotification::ScheduleOperator(operator_id)
                                ).await;
                            }
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
                        _ => {},
                    }
                }
            }
        }
    }
}
