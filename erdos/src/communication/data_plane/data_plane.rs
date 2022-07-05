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
        control_plane::notifications::WorkerAddress, errors::CommunicationError, EhloMetadata,
        InterWorkerMessage,
    },
    dataflow::{
        graph::{AbstractStreamT, Job},
        stream::StreamId,
    },
    node::WorkerId,
};

use super::{
    codec::MessageCodec,
    notifications::{DataPlaneNotification, StreamType},
    worker_connection::WorkerConnection,
    StreamManager,
};

/// A data structure that manages connections amongst [`Worker`]s, and constructs the Streams
/// used by the `Operator`s to communicate to other `Operator`s.
pub(crate) struct DataPlane {
    /// The ID of the [`Worker`] to whom this `DataPlane` belongs.
    worker_id: WorkerId,
    /// The TCP connection on which the `Worker` is listening for
    /// incoming connections from other `Worker`s.
    worker_connection_listener: TcpListener,
    /// A notification channel from the `Worker` to send notifications
    /// to the `DataPlane`.
    channel_from_worker: UnboundedReceiver<DataPlaneNotification>,
    /// A notification channel from the `DataPlane` to send notifications
    /// to the `Worker`.
    channel_to_worker: UnboundedSender<DataPlaneNotification>,
    /// A notification channel from the [`DataSender`]s and [`DataReceiver`]s
    /// corresponding to the connections to other `Worker`s.
    channel_from_worker_connections: UnboundedReceiver<DataPlaneNotification>,
    /// A notification channel to the `DataSender`s and `DataReceiver`s
    /// corresponding to the connections to other `Worker`s.
    channel_from_worker_connections_tx: UnboundedSender<DataPlaneNotification>,
    /// A mapping from the ID of the `Worker`s to whom this `DataPlane` maintains
    /// connections to their `DataReceiver`s and `DataSender`s.
    connections_to_other_workers: HashMap<WorkerId, WorkerConnection>,
    /// A shared reference to the [`StreamManager`] that contains communication
    /// endpoints to be used by the `Operator`s.
    stream_manager: Arc<Mutex<StreamManager>>,
    /// Caches the Streams that need to be setup upon connection to a Worker
    /// with the given ID.
    worker_to_stream_setup_map: HashMap<WorkerId, Vec<(Job, Box<dyn AbstractStreamT>)>>,
    /// Caches the jobs that are pending before a Stream can be notified as Ready.
    pending_job_setups: HashMap<StreamId, HashSet<Job>>,
}

impl DataPlane {
    /// Initialize a [`DataPlane`].
    ///
    /// # Arguments
    /// - `worker_id`: The ID of the [`Worker`] to whom this `DataPlane` belongs.
    /// - `address`: The IP address where the `DataPlane` listens to connections from
    ///    other `Worker`s.
    /// - `stream_manager`: A shared reference to a `StreamManager` that is populated
    ///    by the `DataPlane` with endpoints which are used by the `Job`s to retrieve
    ///    and send messages.
    /// - `channel_from_worker`: A notification channel to receive notifications from
    ///    the `Worker`.
    /// - `channel_to_worker`: A notification channel to send notifications to the `Worker`.
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

    /// Execute the main loop of the `DataPlane`.
    ///
    /// This method begins listening for connections from other `Worker`s and for notifications
    /// from the `Worker` for whom this `DataPlane` is executing.
    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        tracing::info!(
            "[DataPlane {}] Running data plane for Worker {} at address: {}",
            self.worker_id,
            self.worker_id,
            self.address()
        );
        loop {
            tokio::select! {
                // Respond to incoming TCP connections from other [`Worker`]s.
                worker_connection = self.worker_connection_listener.accept() => {
                    match worker_connection {
                        Ok((worker_stream, worker_address)) => {
                            match self.handle_incoming_worker_connection(
                                    worker_stream,
                                    worker_address
                                ).await {
                                    Ok(connection) => {
                                        self.connections_to_other_workers.insert(
                                            connection.get_id(),
                                            connection,
                                        );
                                    }
                                    Err(error) => {
                                        tracing::warn!("[DataPlane {}] {}", self.worker_id, error);
                                    }
                                }
                        }
                        Err(error) => {
                            tracing::error!(
                                "[DataPlane {}] Received an error when accepting a
                                                    Worker connection: {}",
                                self.worker_id,
                                error,
                            );
                        }
                    }
                }

                // Respond to notifications from the [`Worker`] to whom this [`DataPlane`] belongs.
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
                        _ => {
                            tracing::warn!(
                                "[DataPlane {}] Unsupported notification from Worker: {:?}",
                                self.worker_id,
                                worker_message,
                            )
                        }
                    }
                }

                // Respond to notifications from the `DataSender`s and `DataReceiver`s for the
                // `Worker`s that this `DataPlane` is connected to.
                Some(notification) = self.channel_from_worker_connections.recv() => {
                    if let Err(error) = self.handle_notification_from_worker_connections(
                        notification,
                    ) {
                        tracing::warn!("[DataPlane {}] {}", self.worker_id, error);
                    }
                }

            }
        }
    }

    /// Respond to a notification from other [`Worker`]s.
    ///
    /// # Arguments
    /// - `notification`: The notification received from the [`DataSender`]s and [`DataReceiver`]s
    /// for all the [`WorkerConnection`]s.
    fn handle_notification_from_worker_connections(
        &mut self,
        notification: DataPlaneNotification,
    ) -> Result<(), CommunicationError> {
        match notification {
            DataPlaneNotification::ReceiverInitialized(worker_id) => {
                match self.connections_to_other_workers.get_mut(&worker_id) {
                    Some(worker_connection) => {
                        worker_connection.set_data_receiver_initialized();
                    }
                    None => {
                        return Err(CommunicationError::ProtocolError(format!(
                            "Received a notification for the initialization of a DataReceiver \
                                        for Worker {}, but no such connection was found.",
                            worker_id,
                        )));
                    }
                };
            }
            DataPlaneNotification::SenderInitialized(worker_id) => {
                let worker_connection = match self.connections_to_other_workers.get_mut(&worker_id)
                {
                    Some(worker_connection) => worker_connection,
                    None => {
                        return Err(CommunicationError::ProtocolError(format!(
                            "Received a notification for the initialization of a DataSender \
                                        for Worker {}, but no such connection was found.",
                            worker_id,
                        )));
                    }
                };

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
                                                    stream.source().unwrap(),
                                                    stream.id(),
                                                ),
                                            ) {
                                                return Err(CommunicationError::ProtocolError(
                                                    format!(
                                                        "Received error when notifying Worker of \
                                                        StreamReady for Stream {} and Job {:?}: \
                                                                                        {:?}",
                                                        stream.id(),
                                                        stream.source().unwrap(),
                                                        error
                                                    ),
                                                ));
                                            }
                                            self.pending_job_setups.remove(&stream.id());
                                        }
                                    }
                                    false => {
                                        return Err(CommunicationError::ProtocolError(format!(
                                            "Could not find Job {:?} in pending \
                                            jobs for Stream {}.",
                                            job,
                                            stream.id()
                                        )));
                                    }
                                }
                            }
                            None => {
                                return Err(CommunicationError::ProtocolError(format!(
                                    "Inconsistency between cached streams to be \
                                    setup for Worker {}, Stream {} and Job {:?}.",
                                    worker_id,
                                    stream.id(),
                                    job
                                )));
                            }
                        }
                    }
                }
            }
            DataPlaneNotification::PusherUpdated(stream_id, receiving_job) => {
                // Notify the Worker that the Stream is ready for the given Job.
                if let Err(error) = self
                    .channel_to_worker
                    .send(DataPlaneNotification::StreamReady(receiving_job, stream_id))
                {
                    return Err(CommunicationError::ProtocolError(format!(
                        "Received error when notifying Worker of StreamReady \
                                        for Stream {} and Job {:?}: {:?}",
                        stream_id, receiving_job, error
                    )));
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
            _ => {
                return Err(CommunicationError::ProtocolError(format!(
                    "Received unsupported notification: {:?}",
                    notification
                )));
            }
        }
        Ok(())
    }

    /// Manage an incoming TCP connection from another [`Worker`] at the given address.
    ///
    /// The method performs the handshake procedure defined in
    /// [`EHLO`](crate::communication::InterWorkerMessage::Ehlo) to retrieve the ID of
    /// the remote `Worker`, and returns a new [`WorkerConnection`] object if successful.
    ///
    /// # Arguments:
    /// - `tcp_stream`: The TCP connection initiated by the remote `Worker`.
    /// - `worker_address`: The address of the remote `Worker`.
    async fn handle_incoming_worker_connection(
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
                    if let InterWorkerMessage::Ehlo { metadata } = message {
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
                        return Err(CommunicationError::ProtocolError(format!(
                            "EHLO protocol went wrong with the Worker at address {}.",
                            worker_address
                        )));
                    }
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            return Err(CommunicationError::ProtocolError(format!(
                "No EHLO message received from Worker at address {}.",
                worker_address
            )));
        };

        // Create a new WorkerConnection.
        Ok(WorkerConnection::new(
            other_worker_id,
            worker_sink,
            worker_stream,
            self.channel_from_worker_connections_tx.clone(),
        ))
    }

    /// Initiates a TCP connection to another [`Worker`]'s [`DataPlane`] at the given address.
    ///
    /// # Arguments
    /// - `other_worker_id`: The ID of the [`Worker`] being connected to.
    /// - `worker_address`: The address of the `Worker` being connected to.
    async fn initiate_worker_connection(
        &self,
        other_worker_id: WorkerId,
        worker_address: SocketAddr,
    ) -> Result<WorkerConnection, CommunicationError> {
        let worker_connection = TcpStream::connect(worker_address).await?;
        tracing::debug!(
            "[DataPlane {}] Successfully connected to Worker {} at \
                                                        address {}.",
            self.worker_id,
            other_worker_id,
            worker_address,
        );

        let (mut worker_sink, worker_stream) =
            Framed::new(worker_connection, MessageCodec::new()).split();
        worker_sink
            .send(InterWorkerMessage::Ehlo {
                metadata: EhloMetadata {
                    worker_id: self.worker_id,
                },
            })
            .await?;
        Ok(WorkerConnection::new(
            other_worker_id,
            worker_sink,
            worker_stream,
            self.channel_from_worker_connections_tx.clone(),
        ))
    }

    /// Sets up the `Stream`s for the given [`Job`].
    ///
    /// # Arguments
    /// - `job`: The `Job` for whom the `Stream`s are supposed to be setup.
    /// - `streams`: A collection of `Stream`s corresponding to the `job` that must be initialized.
    async fn setup_streams(
        &mut self,
        job: Job,
        streams: Vec<StreamType>,
    ) -> Result<(), CommunicationError> {
        for stream in streams {
            let notification = match stream {
                StreamType::ReadStream(stream, source_address)
                | StreamType::EgressStream(stream, source_address) => {
                    if self.setup_read_stream(&stream, job, source_address).await? {
                        // If the ReadStream setup was successful, i.e., there are no
                        // remaining [`Pusher`]s to be updated, then notify the Worker
                        // that the Stream is ready.
                        Some(DataPlaneNotification::StreamReady(job, stream.id()))
                    } else {
                        None
                    }
                }
                StreamType::WriteStream(stream, destination_addresses)
                | StreamType::IngressStream(stream, destination_addresses) => {
                    if self.setup_write_stream(&stream, destination_addresses) {
                        // If the WriteStream setup was successful, i.e., there are no
                        // pending connections to be made to other Workers, then notify
                        // the Worker that the Stream is ready.
                        Some(DataPlaneNotification::StreamReady(
                            stream.source().unwrap(),
                            stream.id(),
                        ))
                    } else {
                        None
                    }
                }
            };

            if let Some(notification) = notification {
                tracing::trace!(
                    "[DataPlane {}] Notifying Worker that {:?}.",
                    self.worker_id,
                    notification,
                );
                self.channel_to_worker.send(notification.clone())?;
            }
        }
        Ok(())
    }

    /// Sets up the [`ReadStream`] given the address of the Job generating the data.
    ///
    /// This method returns `true` if the setup was successful i.e., the `WriteStream`
    /// generating the data was present on the local worker. If the `job` generating the
    /// data is on a separate worker, then the `DataPlane` waits to be notified of a
    /// successful receipt of the `PusherUpdated` notification from the `DataReceiver`.
    ///
    /// # Arguments
    /// - `stream`: An [`AbstractStream`] representation of the [`ReadStream`] to be setup.
    /// - `destination_job`: The `Job` for which the `ReadStream` is being setup (i.e.,
    ///    the `Job` that is consuming the data).
    /// - `source_address`: The address of the `Job` generating the data (i.e., the address
    ///    of the `Job` that is generating the data).
    async fn setup_read_stream(
        &mut self,
        stream: &Box<dyn AbstractStreamT>,
        destination_job: Job,
        source_address: WorkerAddress,
    ) -> Result<bool, CommunicationError> {
        tracing::debug!(
            "[DataPlane {}] Setting up ReadStream {} (ID={}) at address {:?}.",
            self.worker_id,
            stream.name(),
            stream.id(),
            source_address,
        );

        match source_address {
            WorkerAddress::Remote(worker_id, worker_address) => {
                tracing::trace!(
                    "[DataPlane {}] Setting up ReadStream {} (ID={}) for Job {:?} \
                             with its source Job {:?} on the Worker {} ({}).",
                    self.worker_id,
                    stream.name(),
                    stream.id(),
                    destination_job,
                    stream.source().unwrap(),
                    worker_id,
                    worker_address,
                );
                // If there is no connection to the Worker, initiate a new connection.
                if !self.connections_to_other_workers.contains_key(&worker_id) {
                    tracing::trace!(
                        "[DataPlane {}] Initiating a remote connection to Worker {} \
                        ({}) since the source Job {:?} of the ReadStream {} (ID={}) \
                                                        is on that Worker.",
                        self.worker_id,
                        worker_id,
                        worker_address,
                        stream.source().unwrap(),
                        stream.name(),
                        stream.id()
                    );
                    let connection = self
                        .initiate_worker_connection(worker_id, worker_address)
                        .await?;
                    self.connections_to_other_workers
                        .insert(connection.get_id(), connection);
                }

                // Request the stream manager to add an inter-Worker receive endpoint
                // on this Worker connection.
                tracing::trace!(
                    "[DataPlane {}] Adding a RecvEndpoint for the destination Job {:?} \
                                    for Stream {} on a connection to the Worker {}.",
                    self.worker_id,
                    destination_job,
                    stream.id(),
                    worker_id,
                );
                let worker_connection = self
                    .connections_to_other_workers
                    .get_mut(&worker_id)
                    .unwrap();
                let mut stream_manager = self.stream_manager.lock().unwrap();
                stream_manager.add_inter_worker_recv_endpoint(
                    &stream,
                    destination_job,
                    &worker_connection,
                )?;

                // We notify the caller that the Stream is not yet ready because we
                // haven't received a notification from the [`DataReceiver`] that
                // the [`Pusher`] for this endpoint has been correctly installed.
                Ok(false)
            }
            WorkerAddress::Local => {
                tracing::trace!(
                    "[DataPlane {}] Skipping setup of ReadStream {} (ID={}) for Job {:?} \
                                since its source Job {:?} is on the same Worker.",
                    self.worker_id,
                    stream.name(),
                    stream.id(),
                    destination_job,
                    stream.source().unwrap(),
                );

                // We notify the caller that the Stream is ready because there is nothing
                // to be done. The [`RecvEndpoint`] for this Stream will be constructed in
                // the [`setup_write_stream`] method.
                Ok(true)
            }
        }
    }

    /// Sets up the [`WriteStream`] given the addresses of the destination [`Worker`]s.
    ///
    /// The method returns `true` if the setup was successful to all the destinations
    /// registered with the Stream. If no address or no already existing connection is
    /// found for the destination, then the setup is unsucessful, with the potential
    /// of succeeding at a later time.
    ///
    /// # Arguments
    /// - `stream`: An [`AbstractStream`] representation of the `WriteStream` to be setup.
    /// - `destination_addresses`: A mapping from the [`Job`]s that this `WriteStream`
    ///    publishes data to along with the addresses of their destination `Worker`s.
    fn setup_write_stream(
        &mut self,
        stream: &Box<dyn AbstractStreamT>,
        destination_addresses: HashMap<Job, WorkerAddress>,
    ) -> bool {
        tracing::trace!(
            "[DataPlane {}] Setting up WriteStream {} (ID={}) for addresses {:?}.",
            self.worker_id,
            stream.name(),
            stream.id(),
            destination_addresses
        );

        let mut stream_manager = self.stream_manager.lock().unwrap();

        let mut setup_successful = true;
        for destination in stream.destinations() {
            let destination_address = match destination_addresses.get(&destination) {
                Some(destination_address) => destination_address,
                None => {
                    tracing::warn!(
                        "[DataPlane {}] Could not find an address for the destination Job {:?}.",
                        self.worker_id,
                        destination,
                    );
                    // TODO (Sukrit): The current system provides no way to recover from this error.
                    // There should be a way for Workers to request placements from Leaders.
                    setup_successful = false;
                    continue;
                }
            };
            match destination_address {
                WorkerAddress::Remote(worker_id, _) => {
                    match self.connections_to_other_workers.get(worker_id) {
                        Some(worker_connection) => {
                            tracing::trace!(
                                "[DataPlane {}] Adding a SendEndpoint for Stream {} on an \
                                        already existing connection to the Worker {}.",
                                self.worker_id,
                                stream.id(),
                                worker_id,
                            );
                            // There already exists a connection to this Worker, register
                            // a new SendEndpoint atop this connection.
                            stream_manager.add_inter_worker_send_endpoint(
                                &stream,
                                destination,
                                worker_connection,
                            )
                        }
                        None => {
                            tracing::trace!(
                                "[DataPlane {}] No existing connection was found to Worker {}. \
                                Caching the endpoint generation till a connection is established.",
                                self.worker_id,
                                worker_id,
                            );
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

                            // Notify the caller that the stream setup is pending.
                            setup_successful = false;
                        }
                    }
                }
                WorkerAddress::Local => {
                    tracing::trace!(
                        "[DataPlane {}] Adding an intra-worker endpoint for \
                                                Stream {} and Job {:?}.",
                        self.worker_id,
                        stream.id(),
                        destination,
                    );
                    stream_manager.add_intra_worker_endpoint(&stream, destination);
                }
            }
        }

        setup_successful
    }

    /// Retrieves the address that the [`DataPlane`] is listening on.
    ///
    /// This method is useful in case the `DataPlane` is requested to initialize
    /// itself on a randomly-chosen port.
    pub fn address(&self) -> SocketAddr {
        self.worker_connection_listener.local_addr().unwrap()
    }
}
