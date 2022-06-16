use std::net::SocketAddr;

use tokio::{
    sync::broadcast::{self, Sender},
    task::JoinHandle,
};

use crate::{
    communication::{CommunicationError, DriverNotification},
    node::{Resources, WorkerNode},
    Uuid, dataflow::graph::JobGraph,
};

pub type ClientId = Uuid;

/// A [`Client`] is used by driver applications to submit ERDOS applications
/// to the ERDOS Leader, and query their execution progres.
pub struct Client {
    client_handle: Sender<DriverNotification>,
    client_id: ClientId,
    worker_task: JoinHandle<Result<(), CommunicationError>,
}

impl Client {
    pub(crate) fn new(leader_address: SocketAddr) -> Self {
        // Initialize a Worker node with no resources, and maintain a
        // connection to the
        let worker_resources = Resources::new(0, 0);
        let (client_tx, client_rx) = broadcast::channel(100);
        let mut worker_node = WorkerNode::new(leader_address, worker_resources, client_rx);
        let worker_id = worker_node.get_id();
        let worker_task = tokio::spawn(async move {
            worker_node.run().await
        });
        Self {
            client_id: worker_id,
            client_handle: client_tx,
            worker_task,
        }
    }

    pub fn submit(&self, job_graph: &JobGraph) -> ClientId {
        ClientId::nil()
    }
}
