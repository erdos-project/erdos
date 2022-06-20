// TODO(Sukrit): Rename this to worker.rs once the merge is complete.

use std::{collections::HashMap, net::SocketAddr};

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::mpsc::Receiver};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        CommunicationError, ControlPlaneCodec, DriverNotification, LeaderNotification,
        WorkerNotification,
    },
    dataflow::graph::{InternalGraph, JobGraph},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Resources {
    num_cpus: usize,
    num_gpus: usize,
}

impl Resources {
    pub fn new(num_cpus: usize, num_gpus: usize) -> Self {
        Self { num_cpus, num_gpus }
    }

    pub fn empty() -> Self {
        Self {
            num_cpus: 0,
            num_gpus: 0,
        }
    }
}

pub(crate) struct WorkerNode {
    worker_id: usize,
    leader_address: SocketAddr,
    resources: Resources,
    driver_notification_rx: Receiver<DriverNotification>,
    job_graphs: HashMap<String, JobGraph>,
}

impl WorkerNode {
    pub fn new(
        worker_id: usize,
        leader_address: SocketAddr,
        resources: Resources,
        driver_notification_rx: Receiver<DriverNotification>,
    ) -> Self {
        Self {
            worker_id,
            leader_address,
            resources,
            driver_notification_rx,
            job_graphs: HashMap::new(),
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
                                LeaderNotification::ScheduleOperator(job_name, operator_id) => {
                                    if let Some(job_graph) = self.job_graphs.get(&job_name) {
                                        if let Some(operator) =
                                            job_graph.get_operator(&operator_id)
                                        {
                                            tracing::debug!(
                                                "The Worker with ID: {:?} received operator \
                                                {} with ID: {:?}.",
                                                self.worker_id,
                                                operator.config.name.unwrap_or(
                                                    "UnnamedOperator".to_string()
                                                ),
                                                operator_id
                                            );
                                            // TODO: Handle Operator
                                            let _ = leader_tx.send(
                                                WorkerNotification::OperatorReady(operator_id)
                                            ).await?;
                                        } else {
                                            tracing::error!(
                                                "The Operator with ID: {} was not found in \
                                                JobGraph {}",
                                                operator_id,
                                                job_name
                                            );
                                        }
                                    } else {
                                        tracing::error!(
                                            "The JobGraph {} was not registered on the Worker {}",
                                            job_name,
                                            self.worker_id
                                        );
                                    }
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
                Some(driver_notification) = self.driver_notification_rx.recv() => {
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
                        DriverNotification::RegisterGraph(job_graph) => {
                            // Save the JobGraph.
                            let job_graph_name = job_graph.get_name().clone().to_string();
                            tracing::debug!("The Worker {} received the JobGraph {}.", self.worker_id, job_graph_name);
                            self.job_graphs.insert(job_graph_name, job_graph);
                        }
                        DriverNotification::SubmitGraph(job_graph_name) => {
                            // Retrieve the JobGraph and communicate an Abstract version
                            // of the graph to the Leader.
                            if let Some(job_graph) = self.job_graphs.get(&job_graph_name) {
                                let internal_graph = job_graph.clone().into();
                                if let Err(error) = leader_tx.send(
                                    WorkerNotification::SubmitGraph(job_graph_name.clone(), internal_graph)
                                ).await {
                                    tracing::error!(
                                        "Worker {} received an error when sending Abstract \
                                        Graph message to Leader: {:?}",
                                        self.worker_id,
                                        error
                                    );
                                };
                            } else {
                                tracing::error!(
                                    "Worker {} found no JobGraph with name {}",
                                    self.worker_id,
                                    job_graph_name,
                                )
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn get_id(&self) -> usize {
        self.worker_id.clone()
    }
}
