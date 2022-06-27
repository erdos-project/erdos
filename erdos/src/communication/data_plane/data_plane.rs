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
    dataflow::{
        graph::{AbstractStreamT, Job},
        stream::StreamId,
    },
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
    /// Caches the Streams that need to be setup upon connection to a Worker
    /// with the given ID.
    worker_to_stream_setup_map: HashMap<WorkerId, Vec<Box<dyn AbstractStreamT>>>,
    /// Bookkeeping to ensure that all channels for a Stream are correctly initialized.
    stream_to_channel_setup_map: HashMap<StreamId, HashMap<Job, bool>>,
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
            worker_to_stream_setup_map: HashMap::new(),
            stream_to_channel_setup_map: HashMap::new(),
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
                        DataPlaneNotification::SetupWriteStream(stream, worker_addresses) => {
                            tracing::debug!("[DataPlane {}] Requested to setup WriteStream (ID={}) for {:?}.", self.worker_id, stream.id(), worker_addresses);
                        }
                        _ => unreachable!(),
                    }
                }

                // Handle messages from the Senders and the Receivers.
                Some(notification) = self.channel_from_worker_connections.recv() => {
                    self.handle_notification_from_worker_connections(notification);
                }

            }
        }
    }

    fn handle_notification_from_worker_connections(&mut self, notification: DataPlaneNotification) {
        match notification {
            DataPlaneNotification::ReceiverInitialized(worker_id) => {
                self.connections_to_other_workers
                    .get_mut(&worker_id)
                    .unwrap()
                    .set_data_receiver_initialized();
            }
            DataPlaneNotification::SenderInitialized(worker_id) => {
                self.connections_to_other_workers
                    .get_mut(&worker_id)
                    .unwrap()
                    .set_data_sender_initialized();
            }
            DataPlaneNotification::PusherUpdated(stream_id, job) => {
                // Update the status of the Job for the given stream.
                match self.stream_to_channel_setup_map.get_mut(&stream_id) {
                    Some(job_status) => match job_status.get_mut(&job) {
                        Some(initialized) => {
                            *initialized = true;

                            // TODO (Sukrit): If the status of all Jobs is initialized now, 
                            // tell the Worker that the Stream was successfully initialized.
                        }
                        None => {
                            tracing::warn!(
                                "[DataPlane {}] Could nto find the status for the Job {:?} \
                                    for Stream {} for which the DataPlane was notified of a \
                                    PusherUpdate.",
                                self.worker_id,
                                job,
                                stream_id
                            );
                        }
                    },
                    None => {
                        tracing::warn!(
                            "[DataPlane {}] Could not find the status for Jobs for Stream {} for \
                            which the DataPlane was notified of a PusherUpdate.",
                            self.worker_id,
                            stream_id
                        );
                    }
                }
            }
            _ => unreachable!(),
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
            "[DataPlane {}] Setting up ReadStream {} (ID={}) at address {:?}.",
            self.worker_id,
            stream.name(),
            stream.id(),
            source_address,
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

                // Request the stream manager to add an inter-Worker receive endpoint
                // on this Worker connection.
                let worker_connection = self
                    .connections_to_other_workers
                    .get_mut(&worker_id)
                    .unwrap();
                let mut stream_manager = self.stream_manager.lock().unwrap();
                stream_manager.add_inter_worker_recv_endpoint(
                    &stream,
                    stream.get_source(),
                    &worker_connection,
                );

                // Bookkeep the endpoints required to mark this Stream ready.
                let stream_bookkeeping = self
                    .stream_to_channel_setup_map
                    .entry(stream.id())
                    .or_default();
                stream_bookkeeping.insert(stream.get_source(), false);
            }
            WorkerAddress::Local => todo!(),
        }
        Ok(())
    }

    async fn setup_write_stream(
        &mut self,
        stream: Box<dyn AbstractStreamT>,
        destination_addresses: HashMap<Job, WorkerAddress>,
    ) {
        tracing::debug!(
            "[DataPlane {}] Setting up WriteStream {} (ID={}) for addresses {:?}.",
            self.worker_id,
            stream.name(),
            stream.id(),
            destination_addresses
        );

        let mut stream_manager = self.stream_manager.lock().unwrap();

        for destination in stream.get_destinations() {
            match destination_addresses.get(&destination).unwrap() {
                WorkerAddress::Remote(worker_id, worker_address) => {
                    match self.connections_to_other_workers.get(worker_id) {
                        Some(worker_connection) => {
                            // There already exists a connection to this Worker, register
                            // a new SendEndpoint atop this connection.
                            stream_manager.add_inter_worker_send_endpoint(
                                &stream,
                                destination,
                                worker_connection,
                            )
                        }
                        None => {
                            // Cache the generation of the endpoints until the connection
                            // to the required Worker is established by their receiver.
                            let worker_map = self
                                .worker_to_stream_setup_map
                                .entry(*worker_id)
                                .or_default();
                            worker_map.push(stream.clone());

                            // Bookkeep the endpoints required to mark this Stream ready.
                            let stream_bookkeeping = self
                                .stream_to_channel_setup_map
                                .entry(stream.id())
                                .or_default();
                            stream_bookkeeping.insert(destination, false);
                        }
                    }
                }
                WorkerAddress::Local => todo!(),
            }
        }
    }

    pub fn get_address(&self) -> SocketAddr {
        self.worker_connection_listener.local_addr().unwrap()
    }
}
