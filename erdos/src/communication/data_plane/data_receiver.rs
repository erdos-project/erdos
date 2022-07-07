use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::{stream::SplitStream, StreamExt};
use tokio::{
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        data_plane::notifications::DataPlaneNotification, errors::CommunicationError,
        InterWorkerMessage,
    },
    dataflow::stream::StreamId,
    node::WorkerId,
};

use super::{codec::MessageCodec, pusher::PusherT};

/// Listens on a TCP stream, and pushes messages it receives to operator executors.
pub(crate) struct DataReceiver {
    /// The id of the [`Worker`] the TCP stream is receiving data from.
    worker_id: WorkerId,
    /// The receiver of the Framed TCP stream for the connection to the other Worker.
    tcp_stream: SplitStream<Framed<TcpStream, MessageCodec>>,
    /// Channel where [`DataPlaneNotification`]s are received.
    data_plane_notification_rx: UnboundedReceiver<DataPlaneNotification>,
    /// Channel where notifications are communicated to the [`DataPlane`] handler.
    data_plane_notification_tx: UnboundedSender<DataPlaneNotification>,
    /// Mapping between stream id to [`PusherT`] trait objects.
    /// [`PusherT`] trait objects are used to deserialize and send messages to operators.
    stream_id_to_pusher: HashMap<StreamId, Arc<Mutex<dyn PusherT>>>,
}

impl DataReceiver {
    pub(crate) fn new(
        worker_id: WorkerId,
        tcp_stream: SplitStream<Framed<TcpStream, MessageCodec>>,
        data_plane_notification_rx: UnboundedReceiver<DataPlaneNotification>,
        data_plane_notification_tx: UnboundedSender<DataPlaneNotification>,
    ) -> Self {
        Self {
            worker_id,
            tcp_stream,
            data_plane_notification_rx,
            data_plane_notification_tx,
            stream_id_to_pusher: HashMap::new(),
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify the Worker that the DataReceiver is initialized.
        self.data_plane_notification_tx
            .send(DataPlaneNotification::ReceiverInitialized(self.worker_id))
            .map_err(CommunicationError::from)?;

        tracing::debug!(
            "[DataReceiver for Worker {}] Initialized DataReceiver.",
            self.worker_id
        );

        // Listen for updates to the Pusher and messages on the TCP stream.
        loop {
            tokio::select! {
                // We want to bias the select towards Pusher updates in order to
                // minimize any lost messages.
                biased;
                Some(notification) = self.data_plane_notification_rx.recv() => {
                    self.handle_data_plane_notification(notification);
                }

                // Listen for messages on the TCP connection, and send them to the Pusher
                // corresponding to the StreamId specified in the metadata of the message.
                Some(message) = self.tcp_stream.next() => {
                    match message {
                        Ok(message) => {
                            // Read the Metadata from the Message.
                            let (metadata, bytes) = match message {
                                InterWorkerMessage::Serialized { metadata, bytes } => {
                                    (metadata, bytes)
                                }
                                _ => unreachable!(),
                            };

                            // Find the corresponding Pusher for the message, and send the bytes.
                            match self.stream_id_to_pusher.get_mut(&metadata.stream_id) {
                                Some(pusher) => {
                                    let mut pusher = pusher.lock().unwrap();
                                    if let Err(error) = pusher.send_from_bytes(bytes) {
                                        return Err(error);
                                    }
                                }
                                None => tracing::error!(
                                    "[DataReceiver for Worker {}] Could not find a Pusher \
                                                                    for StreamID: {}.",
                                    self.worker_id,
                                    metadata.stream_id,
                                ),
                            }
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
    }

    fn handle_data_plane_notification(&mut self, notification: DataPlaneNotification) {
        match notification {
            // Update the StreamID to dyn PusherT mapping if we have an update.
            DataPlaneNotification::InstallPusher(stream_id, stream_pusher) => {
                {
                    let pusher = stream_pusher.lock().unwrap();
                    tracing::debug!(
                        "[DataReceiver for Worker {}] Installed Pusher {:?}.",
                        self.worker_id,
                        pusher
                    );
                }
                self.stream_id_to_pusher.insert(stream_id, stream_pusher);
            }
            DataPlaneNotification::UpdatePusher(stream_id, receiving_job) => {
                let pusher = self
                    .stream_id_to_pusher
                    .get(&stream_id)
                    .unwrap()
                    .lock()
                    .unwrap();
                tracing::debug!(
                    "[DataReceiver for Worker {}] Received a Pusher update \
                                notification for {:?} to Job {:?}.",
                    self.worker_id,
                    pusher,
                    receiving_job
                );

                // Notify the DataPlane of the successful installation.
                match pusher.contains_endpoint(&receiving_job) {
                    true => {
                        if let Err(error) = self.data_plane_notification_tx.send(
                            DataPlaneNotification::PusherUpdated(stream_id, receiving_job),
                        ) {
                            tracing::error!(
                                "[DataReceiver for Worker {}] Error sending the PusherUpdated \
                                notification to the DataPlane for Stream {:?} to Job {:?}: {:?}",
                                self.worker_id,
                                stream_id,
                                receiving_job,
                                error,
                            );
                        }
                    }
                    false => {
                        tracing::warn!(
                            "[DataReceiver for Worker {}] The DataReceiver was notified of the \
                            update of the Pusher for Stream {} and an endpoint for Job {:?}, \
                            but none was found.",
                            self.worker_id,
                            stream_id,
                            receiving_job,
                        );
                    }
                }
            }
            _ => {}
        }
    }
}
