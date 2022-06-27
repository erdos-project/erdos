use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        control_plane::notifications::WorkerAddress, CommunicationError, EhloMetadata,
        InterProcessMessage, MessageCodec,
    },
    dataflow::graph::{AbstractStreamT, Job},
    node::WorkerId,
    scheduler::channel_manager::ChannelManager,
};

use super::{notifications::DataPlaneNotification, worker_connection::WorkerConnection};

/// [`DataPlane`] manages the connections amongst Workers, and enables
/// [`Worker`]s to communicate data messages to each other.
pub(crate) struct DataPlane {
    worker_id: usize,
    worker_connection_listener: TcpListener,
    channel_from_worker: UnboundedReceiver<DataPlaneNotification>,
    channel_to_worker: UnboundedSender<DataPlaneNotification>,
    channel_from_worker_connections: UnboundedReceiver<DataPlaneNotification>,
    channel_from_worker_connections_tx: UnboundedSender<DataPlaneNotification>,
    connections_to_other_workers: HashMap<WorkerId, WorkerConnection>,
    stream_manager: Arc<Mutex<ChannelManager>>,
}

impl DataPlane {
    pub async fn new(
        worker_id: usize,
        address: SocketAddr,
        channel_from_worker: UnboundedReceiver<DataPlaneNotification>,
        channel_to_worker: UnboundedSender<DataPlaneNotification>,
    ) -> Result<Self, CommunicationError> {
        // Bind to the address that the DataPlane should be working on.
        let worker_connection_listener = TcpListener::bind(address).await?;

        // Construct the notification channels between the DataPlane and the threads
        // that send and receive data to/from the other Workers.
        let (channel_from_worker_connections_tx, channel_from_worker_connections) =
            mpsc::unbounded_channel();

        Ok(Self {
            worker_id,
            worker_connection_listener,
            channel_from_worker,
            channel_to_worker,
            channel_from_worker_connections,
            channel_from_worker_connections_tx,
            connections_to_other_workers: HashMap::new(),
            stream_manager: Arc::new(Mutex::new(ChannelManager::default())),
        })
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        tracing::info!(
            "[DataPlane {}] Running data plane for Worker {} at address: {}",
            self.worker_id,
            self.worker_id,
            self.get_address()
        );
        loop {
            tokio::select! {
                // Handle incoming connections from other workers.
                worker_connection = self.worker_connection_listener.accept() => {
                    match worker_connection {
                        Ok((worker_stream, worker_address)) => {
                            match self.handle_incoming_worker_connections(worker_stream, worker_address).await {
                                Ok(connection) => {
                                    self.connections_to_other_workers.insert(connection.get_id(), connection);
                                }
                                Err(_) => todo!(),
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                "[DataPlane {}] Received an error when handling a \
                                                    Worker connection: {}",
                                self.worker_id,
                                error,
                            );
                        }
                    }
                }

                // Handle messages from the Worker node.
                Some(worker_message) = self.channel_from_worker.recv() => {
                    match worker_message {
                        DataPlaneNotification::SetupReadStream(stream, worker_address) => {
                            let stream_id = stream.id();
                            let stream_name = stream.name();
                            match self.setup_read_stream(stream, worker_address).await {
                                Ok(()) => {}
                                Err(error) => {
                                    tracing::error!("[DataPlane {}] Received error when setting up Stream {} (ID={}): {:?}", self.worker_id, stream_name, stream_id, error);
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }

                // Handle messages from the Senders and the Receivers.
                Some(notification) = self.channel_from_worker_connections.recv() => {
                    match notification {
                        DataPlaneNotification::ReceiverInitialized(worker_id) => {
                            self.connections_to_other_workers.get_mut(&worker_id).unwrap().set_data_receiver_initialized();
                        }
                        DataPlaneNotification::SenderInitialized(worker_id) => {
                            self.connections_to_other_workers.get_mut(&worker_id).unwrap().set_data_sender_initialized();
                        }
                        _ => unreachable!(),
                    }
                }

            }
        }
    }

    async fn handle_incoming_worker_connections(
        &self,
        tcp_stream: TcpStream,
        worker_address: SocketAddr,
    ) -> Result<WorkerConnection, CommunicationError> {
        // Split the TCP stream into a Sink and Stream, and perform the EHLO handshake.
        let (worker_sink, mut worker_stream) =
            Framed::new(tcp_stream, MessageCodec::default()).split();
        let other_worker_id = if let Some(result) = worker_stream.next().await {
            match result {
                Ok(message) => {
                    if let InterProcessMessage::Ehlo { metadata } = message {
                        let other_worker_id = metadata.worker_id;
                        tracing::debug!(
                            "[DataPlane {}] Received an incoming connection from \
                                                    Worker {} from address {}.",
                            self.worker_id,
                            other_worker_id,
                            worker_address
                        );
                        other_worker_id
                    } else {
                        tracing::error!(
                            "[DataPlane {}] The EHLO procedure went wrong with \
                                                    Worker at address {}!",
                            self.worker_id,
                            worker_address,
                        );
                        return Err(CommunicationError::ProtocolError);
                    }
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            unreachable!()
        };
        Ok(WorkerConnection::new(
            other_worker_id,
            worker_sink,
            worker_stream,
            self.channel_from_worker_connections_tx.clone(),
        ))
    }

    async fn handle_outgoing_worker_connections(
        &self,
        other_worker_id: usize,
        worker_address: SocketAddr,
    ) -> Result<WorkerConnection, CommunicationError> {
        match TcpStream::connect(worker_address).await {
            Ok(worker_connection) => {
                tracing::debug!(
                    "[DataPlane {}] Successfully connected to Worker {} at \
                                                        address {}.",
                    self.worker_id,
                    other_worker_id,
                    worker_address,
                );

                let (mut worker_sink, worker_stream) =
                    Framed::new(worker_connection, MessageCodec::new()).split();
                let _ = worker_sink
                    .send(InterProcessMessage::Ehlo {
                        metadata: EhloMetadata {
                            worker_id: self.worker_id,
                        },
                    })
                    .await;
                Ok(WorkerConnection::new(
                    other_worker_id,
                    worker_sink,
                    worker_stream,
                    self.channel_from_worker_connections_tx.clone(),
                ))
            }
            Err(error) => {
                tracing::error!(
                    "[DataPlane {}] Received an error when connecting to Worker \
                                                {} at address {}: {:?}",
                    self.worker_id,
                    other_worker_id,
                    worker_address,
                    error,
                );
                Err(error.into())
            }
        }
    }

    async fn setup_read_stream(
        &mut self,
        stream: Box<dyn AbstractStreamT>,
        source_address: WorkerAddress,
    ) -> Result<(), CommunicationError> {
        tracing::debug!(
            "[DataPlane {}] Setting up Stream {} (ID={}).",
            self.worker_id,
            stream.name(),
            stream.id()
        );

        match source_address {
            WorkerAddress::Remote(worker_id, worker_address) => {
                // If there is no connection to the Worker, initiate a new connection.
                if !self.connections_to_other_workers.contains_key(&worker_id) {
                    let connection = self
                        .handle_outgoing_worker_connections(worker_id, worker_address)
                        .await?;
                    self.connections_to_other_workers
                        .insert(connection.get_id(), connection);
                }

                // Request the stream manager to add an inter-node receive endpoint
                // on this Worker connection.
                let worker_connection = self
                    .connections_to_other_workers
                    .get_mut(&worker_id)
                    .unwrap();
                let mut stream_manager = self.stream_manager.lock().unwrap();
                stream_manager.add_inter_node_recv_endpoint(&stream, &worker_connection);
            }
            WorkerAddress::Local => todo!(),
        }
        Ok(())
    }

    pub fn get_address(&self) -> SocketAddr {
        self.worker_connection_listener.local_addr().unwrap()
    }
}
