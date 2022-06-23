use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::communication::{
    control_plane::notifications::WorkerAddress, CommunicationError, EhloMetadata,
    InterProcessMessage, MessageCodec,
};

use super::notifications::DataPlaneNotification;

/// [`DataPlane`] manages the connections amongst Workers, and enables
/// [`Worker`]s to communicate data messages to each other.
pub(crate) struct DataPlane {
    worker_id: usize,
    worker_connection_listener: TcpListener,
    channel_from_worker: UnboundedReceiver<DataPlaneNotification>,
    channel_to_worker: UnboundedSender<DataPlaneNotification>,
}

impl DataPlane {
    pub async fn new(
        worker_id: usize,
        address: SocketAddr,
        channel_from_worker: UnboundedReceiver<DataPlaneNotification>,
        channel_to_worker: UnboundedSender<DataPlaneNotification>,
    ) -> Result<Self, CommunicationError> {
        let worker_connection_listener = TcpListener::bind(address).await?;
        Ok(Self {
            worker_id,
            worker_connection_listener,
            channel_from_worker,
            channel_to_worker,
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
                            let _ = self
                                .handle_incoming_worker_connections(worker_stream, worker_address)
                                .await;
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
                        DataPlaneNotification::SetupConnections(operator, stream_sources) => {
                            for (stream_id, worker_address) in stream_sources {
                                match worker_address {
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
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_incoming_worker_connections(
        &mut self,
        tcp_stream: TcpStream,
        worker_address: SocketAddr,
    ) -> Result<(), CommunicationError> {
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
        Ok(())
    }

    async fn handle_outgoing_worker_connections(
        &mut self,
        other_worker_id: usize,
        worker_address: SocketAddr,
    ) -> Result<(), CommunicationError> {
        match TcpStream::connect(worker_address).await {
            Ok(worker_connection) => {
                tracing::debug!(
                    "[DataPlane {}] Successfully connected to Worker {} at \
                                                        address {}.",
                    self.worker_id,
                    other_worker_id,
                    worker_address,
                );

                let (mut other_worker_tx, other_worker_rx) =
                    Framed::new(worker_connection, MessageCodec::new()).split();
                let _ = other_worker_tx
                    .send(InterProcessMessage::Ehlo {
                        metadata: EhloMetadata {
                            worker_id: self.worker_id,
                        },
                    })
                    .await;
                Ok(())
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

    pub fn get_address(&self) -> SocketAddr {
        self.worker_connection_listener.local_addr().unwrap()
    }
}
