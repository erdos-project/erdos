use std::net::SocketAddr;

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

use crate::communication::{
    CommunicationError, ControlPlaneCodec, DriverNotification, LeaderNotification, WorkerId,
    WorkerNotification,
};

/// The [`InterThreadMessage`] enum defines the messages that the different
/// spawned tasks may communicate back to the main loop of the [`LeaderNode`].
#[derive(Debug, Clone)]
enum InterThreadMessage {
    WorkerInitialized(WorkerId),
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
}

impl LeaderNode {
    pub fn new(address: SocketAddr, driver_notification_rx: Receiver<DriverNotification>) -> Self {
        Self {
            address,
            driver_notification_rx,
            worker_message_handlers: Vec::new(),
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
                            return Ok(()); }
                        Err(error) => {return Err(CommunicationError::from(error)); },
                    }
                }
                // Handle new messages from the Worker handlers.
                Some(worker_handler_msg) = workers_to_leader_rx.recv() =>  {
                    match worker_handler_msg {
                        InterThreadMessage::WorkerInitialized(worker_id) => {
                            println!("Received identifier from Worker: {}", worker_id);
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

    // Handle messages from the Worker and the Leader.
    loop {
        tokio::select! {
           // Communicate messages received from the Worker to the Leader.
          Some(msg_from_worker) = worker_rx.next() => {
            match msg_from_worker {
                Ok(msg_from_worker) => {
                    match msg_from_worker {
                        WorkerNotification::Initialized(worker_id) => {
                            // Communicate the Worker ID to the Leader.
                            let _ = channel_to_leader.send(InterThreadMessage::WorkerInitialized(worker_id));
                        }
                    }
                },
                Err(error) => {println!("Received error from Worker: {:?}", error);},
            }
           }

           // Communicate messages received from the Leader to the Worker.
           msg_from_leader = channel_from_leader.recv() => {
            match msg_from_leader {
                Ok(msg_from_leader) => { match msg_from_leader {
                    InterThreadMessage::ShutdownAllWorkers => {
                        // The Leader requested all nodes to shutdown.
                        let _ = worker_tx.send(LeaderNotification::Shutdown).await;
                    }
                    _ => {},
                }}
                Err(error) => {}
            }
           }
        }
    }
}
