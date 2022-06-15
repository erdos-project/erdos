use std::{net::SocketAddr, collections::HashMap};

use futures::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        broadcast::{self, Receiver},
        mpsc::{self, UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
};
use tokio_util::codec::Framed;

use crate::{communication::{
    CommunicationError, ControlPlaneCodec, DriverNotification, LeaderNotification, WorkerId,
    WorkerNotification,
}, Uuid, OperatorId};

/// The [`InterThreadMessage`] enum defines the messages that the different
/// spawned tasks may communicate back to the main loop of the [`LeaderNode`].
#[derive(Debug, Clone)]
enum InterThreadMessage {
    WorkerInitialized(WorkerId),
    OperatorScheduled(WorkerId, OperatorId),
    Shutdown(WorkerId),
    ShutdownAllWorkers,
}

pub struct LeaderNode {
    /// The address that the LeaderNode binds to.
    address: SocketAddr,
    /// A Receiver corresponding to a channel between the Driver and the Leader.
    driver_notification_rx: Receiver<DriverNotification>,
    /// A Vector containing the message handlers corresponding to each Worker.
    worker_message_handlers: Vec<JoinHandle<()>>,
    /// A Vector containing Uuids of workers connected to the Leader.
    worker_ids: Vec<Uuid>,
    /// A HashMap that stores which OperatorIds are ready
    operator_ready: HashMap<OperatorId, bool>,
}

impl LeaderNode {
    pub fn new(address: SocketAddr, driver_notification_rx: Receiver<DriverNotification>) -> Self {
        Self {
            address,
            driver_notification_rx,
            worker_message_handlers: Vec::new(),
            worker_ids: Vec::new(),
            operator_ready: HashMap::new(),
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
                            println!("Received a connection from address: {}", worker_address);
                            let leader_to_worker_broadcast_channel = leader_to_workers_tx.subscribe();
                            let worker_handler = tokio::spawn(handle_worker(
                                worker_stream,
                                workers_to_leader_tx.clone(),
                                leader_to_worker_broadcast_channel,
                            ));
                            self.worker_message_handlers.push(worker_handler);
                        }
                        Err(error) => {return Err(CommunicationError::from(error));}
                    }
                }
                // Handle new messages from the drivers.
                driver_notification = self.driver_notification_rx.recv() => {
                    match driver_notification {
                        Ok(DriverNotification::Shutdown) => {
                            // Ask all Workers to shutdown.
                            let _ = leader_to_workers_tx.send(InterThreadMessage::ShutdownAllWorkers);
                            return Ok(()); 
                        }
                        Err(error) => {return Err(CommunicationError::from(error)); },
                    }
                }
                // Handle new messages from the Worker handlers.
                Some(worker_handler_msg) = workers_to_leader_rx.recv() =>  {
                    match worker_handler_msg {
                        InterThreadMessage::WorkerInitialized(worker_id) => {
                            println!("Received identifier from Worker: {}", worker_id);
                            self.worker_ids.push(worker_id);
                        }
                        InterThreadMessage::Shutdown(worker_id) => {
                            println!("Received shutdown from Worker: {}", worker_id);
                        }
                        _ => {todo!();}
                    }
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
        ControlPlaneCodec::<LeaderNotification, WorkerNotification>::new(),
    )
    .split();

    // WorkerId for this handler.
    let mut handle_worker_id = None;

    // Initial Worker-Leader handshake.
    match worker_rx.next().await {
        Some(Ok(msg_from_worker)) => {
            match msg_from_worker {
                WorkerNotification::Initialized(worker_id) => {
                    // Communicate the Worker ID to the Leader.
                    let _ = channel_to_leader.send(InterThreadMessage::WorkerInitialized(worker_id));
                    handle_worker_id = Some(worker_id);
                }
                _ => {println!("Received other message before Worker initialized.");},
            }
        },
        _ => {println!("Recieved incorrect notification.");},
    }

    // Handle messages from the Worker and the Leader.
    loop {
        tokio::select! {
           // Communicate messages received from the Leader to the Worker.
            msg_from_leader = channel_from_leader.recv() => {
                match msg_from_leader {
                    Ok(msg_from_leader) => { match msg_from_leader {
                        InterThreadMessage::OperatorScheduled(worker_id, operator_id) => {
                            if handle_worker_id.unwrap() == worker_id {
                                let _ = worker_tx.send(LeaderNotification::Operator(operator_id)).await;
                            }
                        }
                        InterThreadMessage::ShutdownAllWorkers => {
                            // The Leader requested all nodes to shutdown.
                            let _ = worker_tx.send(LeaderNotification::Shutdown).await;
                        }
                        _ => {},
                    }}
                    Err(error) => {}
                }
            },
            Some(msg_from_worker) = worker_rx.next() => {
                match msg_from_worker {
                    Ok(msg_from_worker) => { match msg_from_worker {
                        WorkerNotification::OperatorReady(operator_id) => {
                            println!("{:?} operator is ready.", operator_id);
                        },
                        _ => {}
                    }},
                    Err(error) => {}
                }
            }
        }
    }
}
