// TODO(Sukrit): Rename this to worker.rs once the merge is complete.

use std::{collections::HashMap, net::SocketAddr};

use futures::{stream::SplitSink, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        control_plane::{
            notifications::{DriverNotification, LeaderNotification, WorkerNotification},
            ControlPlaneCodec,
        },
        data_plane::{data_plane::DataPlane, notifications::DataPlaneNotification},
        CommunicationError,
    },
    dataflow::graph::JobGraph,
    node::Resources,
};

use super::WorkerId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkerState {
    id: WorkerId,
    address: SocketAddr,
    resources: Resources,
}

impl WorkerState {
    fn new(id: WorkerId, address: SocketAddr, resources: Resources) -> Self {
        Self {
            id,
            address,
            resources,
        }
    }

    pub(crate) fn get_address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn get_id(&self) -> WorkerId {
        self.id
    }
}

pub(crate) struct WorkerNode {
    worker_id: WorkerId,
    leader_address: SocketAddr,
    resources: Resources,
    driver_notification_rx: Receiver<DriverNotification>,
    job_graphs: HashMap<String, JobGraph>,
}

impl WorkerNode {
    pub fn new(
        worker_id: WorkerId,
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
        tracing::debug!("[Worker {}] Initialized Worker.", self.worker_id);
        let leader_connection = TcpStream::connect(self.leader_address).await?;
        let (mut leader_tx, mut leader_rx) = Framed::new(
            leader_connection,
            ControlPlaneCodec::<WorkerNotification, LeaderNotification>::default(),
        )
        .split();

        // Initialize the Data layer on a randomly-assigned port.
        // The data layer is used to retrieve the dataflow messages from other operators.
        let (mut channel_to_data_plane_tx, channel_to_data_plane_rx) = mpsc::unbounded_channel();
        let (channel_from_data_plane_tx, channel_from_data_plane_rx) = mpsc::unbounded_channel();
        let mut data_plane = DataPlane::new(
            self.worker_id,
            "0.0.0.0:0".parse().unwrap(),
            channel_to_data_plane_rx,
            channel_from_data_plane_tx,
        )
        .await?;
        let data_plane_address = data_plane.get_address();
        let data_plane_handle = tokio::spawn(async move { data_plane.run().await });

        // Communicate the ID and data plane address of the Worker to the Leader.
        leader_tx
            .send(WorkerNotification::Initialized(WorkerState::new(
                self.worker_id,
                data_plane_address,
                self.resources.clone(),
            )))
            .await?;
        loop {
            tokio::select! {
                // Handle messages received from the Leader.
                Some(msg_from_leader) = leader_rx.next() => {
                    match msg_from_leader {
                        Ok(msg_from_leader) => {
                            match msg_from_leader {
                                LeaderNotification::Shutdown => {
                                    tracing::debug!(
                                        "[Worker {}] Shutting down.",
                                        self.worker_id
                                    );
                                    return Ok(());
                                }
                                _ => {
                                    self.handle_leader_messages(
                                        msg_from_leader,
                                        &mut channel_to_data_plane_tx,
                                        &mut leader_tx
                                    ).await;
                                }
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                "[Worker {}] Received error when retrieving messages \
                                                            from the Leader: {:?}",
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
                            tracing::info!("[Worker {}] Shutting down.", self.worker_id);
                            if let Err(error) = leader_tx.send(WorkerNotification::Shutdown).await {
                                tracing::error!(
                                    "[Worker {}] Received an error when sending Shutdown message \
                                                                            to Leader: {:?}",
                                    self.worker_id,
                                    error
                                );
                            }
                            tokio::join!(data_plane_handle);
                            return Ok(());
                        }
                        _ => self.handle_driver_messages(driver_notification, &mut leader_tx).await,
                    }
                }
            }
        }
    }

    async fn handle_leader_messages(
        &mut self,
        msg_from_leader: LeaderNotification,
        channel_to_data_plane: &mut UnboundedSender<DataPlaneNotification>,
        leader_tx: &mut SplitSink<
            Framed<TcpStream, ControlPlaneCodec<WorkerNotification, LeaderNotification>>,
            WorkerNotification,
        >,
    ) {
        match msg_from_leader {
            LeaderNotification::ScheduleOperator(job_name, operator_id, worker_addresses) => {
                if let Some(job_graph) = self.job_graphs.get(&job_name) {
                    if let Some(operator) = job_graph.get_operator(&operator_id) {
                        let operator_name = match &operator.config.name {
                            Some(name) => name.clone(),
                            None => "UnnamedOperator".to_string(),
                        };
                        tracing::debug!(
                            "[Worker {}] Received request to schedule {} with ID: {:?}.",
                            self.worker_id,
                            operator_name,
                            operator_id,
                        );
                        // Ask the Data plane to setup the read streams for this operator.
                        for read_stream_id in operator.read_streams {
                            let read_stream = job_graph.get_stream(&read_stream_id).unwrap();
                            let source_job = read_stream.get_source();
                            let _ =
                                channel_to_data_plane.send(DataPlaneNotification::SetupReadStream(
                                    read_stream,
                                    worker_addresses.get(&source_job).unwrap().clone(),
                                ));
                        }

                        // Ask the Data plane to setup the read streams for this operator.
                        // for read_stream_id in operator.read_streams {
                        //     let read_stream = job_graph.get_stream(&read_stream_id).unwrap();
                        //     let _ = channel_to_data_plane.send(DataPlaneNotification::SetupReadStream(
                        //         read_stream,
                        //         worker_addresses.clone(),
                        //     ));
                        // }
                        // TODO: Ask Data Plane to setup write streams for this operator.

                        // TODO: Handle Operator
                        if let Err(error) = leader_tx
                            .send(WorkerNotification::OperatorReady(
                                job_name.clone(),
                                operator_id,
                            ))
                            .await
                        {
                            tracing::error!(
                                "[Worker {}] Could not communicate the Ready status of Operator \
                                        {} from the Job {} to the Leader. Received error {:?}",
                                self.worker_id,
                                operator_id,
                                job_name,
                                error,
                            )
                        }
                    } else {
                        tracing::error!(
                            "[Worker {}] Operator with ID: {} was not found in JobGraph {}",
                            self.worker_id,
                            operator_id,
                            job_name
                        );
                    }
                } else {
                    tracing::error!(
                        "[Worker {}] JobGraph {} was not registered on this Worker.",
                        self.worker_id,
                        job_name
                    );
                }
            }
            LeaderNotification::ExecuteGraph(job_name) => {
                tracing::debug!(
                    "[Worker {}] Executing JobGraph {}.",
                    self.worker_id,
                    job_name,
                );
            }
            // The shutdown arm is unreachable, because it should be handled in the main loop.
            LeaderNotification::Shutdown => unreachable!(),
        }
    }

    async fn handle_driver_messages(
        &mut self,
        driver_notification: DriverNotification,
        leader_tx: &mut SplitSink<
            Framed<TcpStream, ControlPlaneCodec<WorkerNotification, LeaderNotification>>,
            WorkerNotification,
        >,
    ) {
        match driver_notification {
            DriverNotification::RegisterGraph(job_graph) => {
                // Save the JobGraph.
                let job_graph_name = job_graph.get_name().clone().to_string();
                tracing::debug!(
                    "[Worker {}] Registered the JobGraph {}.",
                    self.worker_id,
                    job_graph_name
                );
                self.job_graphs.insert(job_graph_name, job_graph);
            }
            DriverNotification::SubmitGraph(job_graph_name) => {
                // Retrieve the JobGraph and communicate an Abstract version
                // of the graph to the Leader.
                if let Some(job_graph) = self.job_graphs.get(&job_graph_name) {
                    let internal_graph = job_graph.clone().into();
                    if let Err(error) = leader_tx
                        .send(WorkerNotification::SubmitGraph(
                            job_graph_name.clone(),
                            internal_graph,
                        ))
                        .await
                    {
                        tracing::error!(
                            "[Worker {}] Received an error when sending Abstract \
                                                Graph message to Leader: {:?}",
                            self.worker_id,
                            error
                        );
                    };
                } else {
                    tracing::error!(
                        "[Worker {}] Found no JobGraph with name {}.",
                        self.worker_id,
                        job_graph_name,
                    )
                }
            }
            // The shutdown arm is unreachable, because it should be handled in the main loop.
            DriverNotification::Shutdown => unreachable!(),
        }
    }

    pub(crate) fn get_id(&self) -> WorkerId {
        self.worker_id.clone()
    }
}
