use std::{collections::HashMap, net::SocketAddr};

use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        broadcast::{self, Sender},
        mpsc::{self, UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        control_plane::notifications::WorkerAddress, receivers::DataReceiver, senders::DataSender,
        CommunicationError, EhloMetadata, InterProcessMessage, MessageCodec,
    },
    dataflow::Message,
    node::WorkerId,
};

use super::notifications::DataPlaneNotification;

struct WorkerConnection {
    worker_id: WorkerId,
    data_receiver_handle: JoinHandle<Result<(), CommunicationError>>,
    receiver_initialized: bool,
    data_sender_handle: JoinHandle<Result<(), CommunicationError>>,
    sender_initialized: bool,
    channel_to_data_sender: UnboundedSender<InterProcessMessage>,
}

impl WorkerConnection {
    fn new(
        worker_id: WorkerId,
        data_receiver_handle: JoinHandle<Result<(), CommunicationError>>,
        data_sender_handle: JoinHandle<Result<(), CommunicationError>>,
        channel_to_data_sender: UnboundedSender<InterProcessMessage>,
    ) -> Self {
        Self {
            worker_id,
            data_receiver_handle,
            data_sender_handle,
            channel_to_data_sender,
            receiver_initialized: false,
            sender_initialized: false,
        }
    }

    fn get_id(&self) -> WorkerId {
        self.worker_id
    }

    fn set_data_sender_initialized(&mut self) {
        self.sender_initialized = true;
    }

    fn set_data_receiver_initialized(&mut self) {
        self.receiver_initialized = true;
    }

    fn is_initialized(&mut self) -> bool {
        self.sender_initialized && self.receiver_initialized
    }
}

/// [`DataPlane`] manages the connections amongst Workers, and enables
/// [`Worker`]s to communicate data messages to each other.
pub(crate) struct DataPlane {
    worker_id: usize,
    worker_connection_listener: TcpListener,
    channel_from_worker: UnboundedReceiver<DataPlaneNotification>,
    channel_to_worker: UnboundedSender<DataPlaneNotification>,
    channel_to_receivers: Sender<DataPlaneNotification>,
    channel_from_senders_receivers: UnboundedReceiver<DataPlaneNotification>,
    channel_from_senders_receivers_tx: UnboundedSender<DataPlaneNotification>,
    connections_to_other_workers: HashMap<WorkerId, WorkerConnection>,
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
        // that receive data from the other Workers.
        let (channel_to_receivers, _) = broadcast::channel(100);

        // Construct the notification channels between the DataPlane and the threads
        // that send and receive data to/from the other Workers.
        let (channel_from_senders_receivers_tx, channel_from_senders_receivers) =
            mpsc::unbounded_channel();

        Ok(Self {
            worker_id,
            worker_connection_listener,
            channel_from_worker,
            channel_to_worker,
            channel_to_receivers,
            channel_from_senders_receivers,
            channel_from_senders_receivers_tx,
            connections_to_other_workers: HashMap::new(),
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
                        DataPlaneNotification::SetupReadStream(stream, source_address) => {
                            match source_address {
                                WorkerAddress::Remote(worker_id, worker_address) => {
                                    let _ = self
                                        .handle_outgoing_worker_connections(
                                            worker_id,
                                            worker_address,
                                        )
                                        .await;
                                }
                                WorkerAddress::Local => todo!(),
                            }
                        }
                        _ => unreachable!(),
                    }
                }

                // Handle messages from the Senders and the Receivers.
                Some(notification) = self.channel_from_senders_receivers.recv() => {
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

        Ok(self.setup_worker_connection(other_worker_id, worker_stream, worker_sink))
    }

    async fn handle_outgoing_worker_connections(
        &mut self,
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
                Ok(self.setup_worker_connection(other_worker_id, worker_stream, worker_sink))
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

    fn setup_worker_connection(
        &self,
        worker_id: WorkerId,
        worker_stream: SplitStream<Framed<TcpStream, MessageCodec>>,
        worker_sink: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
    ) -> WorkerConnection {
        // Create and execute a DataReceiver for the connection to this Worker.
        let mut data_receiver = DataReceiver::new(
            worker_id,
            worker_stream,
            self.channel_to_receivers.subscribe(),
            self.channel_from_senders_receivers_tx.clone(),
        );
        let data_receiver_handle = tokio::spawn(async move { data_receiver.run().await });

        // Create and execute a DataSender for the connection to this Worker.
        let (data_message_sender_tx, data_message_sender_rx) = mpsc::unbounded_channel();
        let mut data_sender = DataSender::new(
            worker_id,
            worker_sink,
            data_message_sender_rx,
            self.channel_from_senders_receivers_tx.clone(),
        );
        let data_sender_handle = tokio::spawn(async move { data_sender.run().await });

        WorkerConnection::new(
            worker_id,
            data_receiver_handle,
            data_sender_handle,
            data_message_sender_tx,
        )
    }

    pub fn get_address(&self) -> SocketAddr {
        self.worker_connection_listener.local_addr().unwrap()
    }
}
