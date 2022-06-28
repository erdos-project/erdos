use std::{
    collections::{HashMap, HashSet},
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
};

use super::{
    notifications::{DataPlaneNotification, StreamType},
    worker_connection::WorkerConnection, StreamManager,
};

/// [`DataPlane`] manages the connections amongst Workers, and enables
/// [`Worker`]s to communicate data messages to each other.
pub(crate) struct DataPlane {
    worker_id: WorkerId,
    worker_connection_listener: TcpListener,
    channel_from_worker: UnboundedReceiver<DataPlaneNotification>,
    channel_to_worker: UnboundedSender<DataPlaneNotification>,
    channel_from_worker_connections: UnboundedReceiver<DataPlaneNotification>,
    channel_from_worker_connections_tx: UnboundedSender<DataPlaneNotification>,
    connections_to_other_workers: HashMap<WorkerId, WorkerConnection>,
    stream_manager: Arc<Mutex<StreamManager>>,
    /// Caches the Streams that need to be setup upon connection to a Worker
    /// with the given ID.
    worker_to_stream_setup_map: HashMap<WorkerId, Vec<(Job, Box<dyn AbstractStreamT>)>>,
    /// Caches the jobs that are pending before a Stream can be notified as Ready.
    pending_job_setups: HashMap<StreamId, HashSet<Job>>,
}

impl DataPlane {
    pub async fn new(
        worker_id: WorkerId,
        address: SocketAddr,
        stream_manager: Arc<Mutex<StreamManager>>,
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
            stream_manager,
            worker_to_stream_setup_map: HashMap::new(),
            pending_job_setups: HashMap::new(),
        })
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        tracing::info!(
            "[DataPlane {}] Running data plane for Worker {} at address: {}",
            self.worker_id,
            self.worker_id,
            self.address()
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
                        DataPlaneNotification::SetupStreams(job, streams) => {
                            if let Err(error) = self.setup_streams(job, streams).await {
                                tracing::warn!(
                                    "[DataPlane {}] Received error when setting up streams \
                                    for the Job {:?}: {:?}",
                                    self.worker_id,
                                    job,
                                    error,
                                );
                            }
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
                let worker_connection = self
                    .connections_to_other_workers
                    .get_mut(&worker_id)
                    .unwrap();

                // Mark the sender as initialized and install any cached endpoints for this node.
                worker_connection.set_data_sender_initialized();
                if let Some(cached_setups) = self.worker_to_stream_setup_map.get(&worker_id) {
                    let mut stream_manager = self.stream_manager.lock().unwrap();
                    for (job, stream) in cached_setups {
                        stream_manager.add_inter_worker_send_endpoint(
                            stream,
                            job.clone(),
                            &worker_connection,
                        );

                        // Remove the job from the pending job set of the Stream.
                        match self.pending_job_setups.get_mut(&stream.id()) {
                            Some(pending_jobs) => {
                                match pending_jobs.remove(job) {
                                    true => {
                                        // If the set is empty, notify the Worker of the successful
                                        // initialization of this stream for its source job.
                                        if pending_jobs.is_empty() {
                                            if let Err(error) = self.channel_to_worker.send(
                                                DataPlaneNotification::StreamReady(
                                                    stream.get_source(),
                                                    stream.id(),
                                                ),
                                            ) {
                                                tracing::warn!(
                                                    "[DataPlane {}] Received error when notifying Worker \
                                                        of StreamReady for Stream {} and Job {:?}: {:?}",
                                                    self.worker_id,
                                                    stream.id(),
                                                    stream.get_source(),
                                                    error
                                                );
                                            }
                                            self.pending_job_setups.remove(&stream.id());
                                        }
                                    }
                                    false => {
                                        tracing::warn!(
                                            "[DataPlane {}] Could not find Job \
                                            {:?} in pending jobs for Stream {}.",
                                            self.worker_id,
                                            job,
                                            stream.id()
                                        );
                                    }
                                }
                            }
                            None => {
                                tracing::warn!(
                                    "[DataPlane {}] Inconsistency between cached streams to be \
                                    setup for Worker {}, Stream {} and Job {:?}.",
                                    self.worker_id,
                                    worker_id,
                                    stream.id(),
                                    job
                                );
                            }
                        }
                    }
                }
            }
            DataPlaneNotification::PusherUpdated(sending_job, stream_id, receiving_job) => {
                // Notify the Worker that the Stream is ready for the given Job.
                if let Err(error) = self
                    .channel_to_worker
                    .send(DataPlaneNotification::StreamReady(receiving_job, stream_id))
                {
                    tracing::error!(
                        "[DataPlane {}] Received error when notifying Worker of \
                                StreamReady for Stream {} and Job {:?}: {:?}",
                        self.worker_id,
                        stream_id,
                        receiving_job,
                        error
                    );
                } else {
                    tracing::trace!(
                        "[DataPlane {}] Successfully notified Worker of \
                            StreamReady for Stream {} and Job {:?}.",
                        self.worker_id,
                        stream_id,
                        receiving_job
                    );
                }
            }
            _ => unreachable!(),
        }
    }

    async fn handle_incoming_worker_connections(
        &mut self,
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

        // Create a new WorkerConnection.
        Ok(WorkerConnection::new(
            other_worker_id,
            worker_sink,
            worker_stream,
            self.channel_from_worker_connections_tx.clone(),
        ))
    }

    async fn handle_outgoing_worker_connections(
        &self,
        other_worker_id: WorkerId,
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

    async fn setup_streams(
        &mut self,
        job: Job,
        streams: Vec<StreamType>,
    ) -> Result<(), CommunicationError> {
        for stream in streams {
            match stream {
                StreamType::ReadStream(stream, source_address) => {
                    self.setup_read_stream(stream, job, source_address).await?
                }
                StreamType::WriteStream(stream, destination_addresses) => {
                    self.setup_write_stream(stream, destination_addresses);
                }
            }
        }
        Ok(())
    }

    async fn setup_read_stream(
        &mut self,
        stream: Box<dyn AbstractStreamT>,
        destination_job: Job,
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
                if let Err(error) = stream_manager.add_inter_worker_recv_endpoint(
                    &stream,
                    destination_job,
                    &worker_connection,
                ) {
                    tracing::error!(
                        "[DataPlane {}] Received error when setting up ReadStream {} (ID={}): {:?}",
                        self.worker_id,
                        stream.name(),
                        stream.id(),
                        error
                    );
                }
            }
            WorkerAddress::Local => todo!(),
        }
        Ok(())
    }

    fn setup_write_stream(
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
                WorkerAddress::Remote(worker_id, _worker_address) => {
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
                            worker_map.push((destination, stream.clone()));

                            // Save the destination that needs to be setup for the given job.
                            let pending_job_setups =
                                self.pending_job_setups.entry(stream.id()).or_default();
                            pending_job_setups.insert(destination);
                        }
                    }
                }
                WorkerAddress::Local => todo!(),
            }
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.worker_connection_listener.local_addr().unwrap()
    }
}
