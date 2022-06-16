// TODO(Sukrit): Rename this to worker.rs once the merge is complete.

use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::broadcast::Receiver};
use tokio_util::codec::Framed;

use crate::communication::{
    CommunicationError, ControlPlaneCodec, DriverNotification, LeaderNotification, WorkerId,
    WorkerNotification,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resources {
    num_cpus: usize,
    num_gpus: usize,
}

impl Resources {
    pub fn new(num_cpus: usize, num_gpus: usize) -> Self {
        Self { num_cpus, num_gpus }
    }
}

pub struct WorkerNode {
    worker_id: WorkerId,
    leader_address: SocketAddr,
    resources: Resources,
    driver_notification_rx: Receiver<DriverNotification>,
}

impl WorkerNode {
    pub fn new(
        leader_address: SocketAddr,
        resources: Resources,
        driver_notification_rx: Receiver<DriverNotification>,
    ) -> Self {
        Self {
            worker_id: WorkerId::new_v4(),
            leader_address,
            resources,
            driver_notification_rx,
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
            .send(WorkerNotification::Initialized(
                self.worker_id,
                self.resources.clone(),
            ))
            .await?;
        loop {
            tokio::select! {
                // Handle messages received from the Leader.
                Some(msg_from_leader) = leader_rx.next() => {
                    match msg_from_leader {
                        Ok(msg_from_leader) => {
                            match msg_from_leader {
                                LeaderNotification::ScheduleOperator(operator_id) => {
                                    tracing::debug!(
                                        "The Worker with ID: {:?} received operator with ID: {:?}.",
                                        self.worker_id,
                                        operator_id
                                    );
                                    // TODO: Handle Operator
                                    let _ = leader_tx.send(
                                        WorkerNotification::OperatorReady(operator_id)
                                    ).await?;
                                }
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
                }

                // Handle messages received from the Driver.
                Ok(driver_notification) = self.driver_notification_rx.recv() => {
                    match driver_notification {
                        DriverNotification::Shutdown => {
                            tracing::info!("The Worker {} is shutting down.", self.worker_id);
                            if let Err(error) = leader_tx.send(WorkerNotification::Shutdown).await {
                                tracing::error!(
                                    "Worker {} received an error when sending Shutdown message \
                                    to Leader: {:?}",
                                    self.worker_id,
                                    error
                                );
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn get_id(&self) -> WorkerId {
        self.worker_id.clone()
    }
}
