use std::collections::HashMap;

use futures::{future, stream::SplitStream, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        CommunicationError, ControlMessage, InterProcessMessage, MessageCodec, PusherT,
    },
    dataflow::stream::StreamId,
};

/// Listens on a TCP stream, and pushes messages it receives to operator executors.
#[allow(dead_code)]
pub(crate) struct DataReceiver {
    /// The id of the [`Worker`] the TCP stream is receiving data from.
    worker_id: usize,
    /// The receiver of the Framed TCP stream for the Worker connection.
    tcp_stream: SplitStream<Framed<TcpStream, MessageCodec>>,
    /// Broadcast channel where [`PusherT`] objects are received from [`Worker`]s.
    stream_pusher_update_rx: broadcast::Receiver<(StreamId, Box<dyn PusherT>)>,
    /// Mapping between stream id to [`PusherT`] trait objects.
    /// [`PusherT`] trait objects are used to deserialize and send messages to operators.
    stream_id_to_pusher: HashMap<StreamId, Box<dyn PusherT>>,
    /// MPSC channel to communicate messages to the Worker.
    channel_to_worker_tx: mpsc::Sender<ControlMessage>,
}

impl DataReceiver {
    pub(crate) async fn new(
        worker_id: usize,
        tcp_stream: SplitStream<Framed<TcpStream, MessageCodec>>,
        stream_pusher_update_rx: broadcast::Receiver<(StreamId, Box<dyn PusherT>)>,
        channel_to_worker_tx: mpsc::Sender<ControlMessage>,
    ) -> Self {
        Self {
            worker_id,
            tcp_stream,
            stream_pusher_update_rx,
            stream_id_to_pusher: HashMap::new(),
            channel_to_worker_tx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify the Worker that the DataReceiver is initialized.
        self.channel_to_worker_tx
            .send(ControlMessage::DataReceiverInitialized(self.worker_id))
            .await
            .map_err(CommunicationError::from)?;

        // Listen for updates to the Pusher and messages on the TCP stream.
        loop {
            tokio::select! {
                // We want to bias the select towards Pusher updates in order to
                // minimize any lost messages.
                biased;
                Ok(pusher_update) = self.stream_pusher_update_rx.recv() => {
                    // Update the StreamID to dyn PusherT mapping if we have an update.
                    let (stream_id, stream_pusher) = pusher_update;
                    self.stream_id_to_pusher.insert(stream_id, stream_pusher);
                }

                // Listen for messages on the TCP connection, and send them to the Pusher
                // corresponding to the StreamId specified in the metadata of the message.
                Some(message) = self.tcp_stream.next() => {
                    match message {
                        Ok(message) => {
                            // Read the Metadata from the Message.
                            let (metadata, bytes) = match message {
                                InterProcessMessage::Serialized { metadata, bytes } => {
                                    (metadata, bytes)
                                }
                                InterProcessMessage::Deserialized { metadata: _, data: _ } => {
                                    unreachable!()
                                }
                            };

                            // Find the corresponding Pusher for the message, and send the bytes.
                            match self.stream_id_to_pusher.get_mut(&metadata.stream_id) {
                                Some(pusher) => {
                                    if let Err(error) = pusher.send_from_bytes(bytes) {
                                        return Err(error);
                                    }
                                }
                                None => tracing::error!(
                                    "[Receiver for Worker {}] Could not find a Pusher \
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
        Ok(())
    }
}

/// Receives TCP messages, and pushes them to operators endpoints.
/// The function receives a vector of framed TCP receiver halves.
/// It launches a task that listens for new messages for each TCP connection.
pub(crate) async fn run_receivers(
    mut receivers: Vec<DataReceiver>,
) -> Result<(), CommunicationError> {
    // Wait for all futures to finish. It will happen only when all streams are closed.
    future::join_all(receivers.iter_mut().map(|receiver| receiver.run())).await;
    Ok(())
}
