// TODO(Sukrit): Rename this to worker.rs once the merge is complete.

use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::broadcast::Receiver};
use tokio_util::codec::Framed;

use crate::communication::{
    CommunicationError, ControlPlaneCodec, DriverNotification, LeaderNotification, WorkerId,
    WorkerNotification,
};

pub struct WorkerNode {
    worker_id: WorkerId,
    leader_address: SocketAddr,
    driver_notification_receiver: Receiver<DriverNotification>,
}

impl WorkerNode {
    pub fn new(
        leader_address: SocketAddr,
        driver_notification_receiver: Receiver<DriverNotification>,
    ) -> Self {
        Self {
            worker_id: WorkerId::new_deterministic(),
            leader_address,
            driver_notification_receiver,
        }
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        // Connect to the Leader node.
        tracing::debug!("Initialized Worker with ID: {}", self.worker_id);
        let leader_connection = TcpStream::connect(self.leader_address).await?;
        let (mut leader_tx, mut leader_rx) = Framed::new(
            leader_connection,
            ControlPlaneCodec::<WorkerNotification, LeaderNotification>::default(),
        )
        .split();

        // Communicate the ID of the Worker to the Leader.
        leader_tx
            .send(WorkerNotification::Initialized(self.worker_id))
            .await?;

        loop {
            // Handle messages received from the Leader.
            tokio::select! {
                Some(msg_from_leader) = leader_rx.next() => {
                    match msg_from_leader {
                        Ok(msg_from_leader) => {
                            match msg_from_leader {
                                LeaderNotification::Shutdown => {
                                    tracing::debug!(
                                        "The Worker with ID: {} is shutting down.", 
                                        self.worker_id
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                "Worker {} received error on the Leader connection: {:?}", 
                                self.worker_id,
                                error
                            );
                        },
                    }
            } }
        }
    }
}
