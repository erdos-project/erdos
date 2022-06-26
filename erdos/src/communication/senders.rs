use futures::{future, stream::SplitSink};
use futures_util::sink::SinkExt;

use tokio::{
    self,
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::communication::{CommunicationError, InterProcessMessage, MessageCodec};

use super::data_plane::notifications::DataPlaneNotification;

#[allow(dead_code)]
/// The [`DataSender`] pulls messages from a FIFO inter-thread channel.
/// The [`DataSender`] services all operators sending messages to a particular
/// Worker which may result in congestion.
pub(crate) struct DataSender {
    /// The ID of the [`Worker`] that the TCP stream is sending data to.
    worker_id: usize,
    /// The sender of the Framed TCP stream for the Worker connection.
    tcp_stream: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
    /// MPSC channel to receive data messages from operators that are to
    /// be forwarded on the underlying TCP stream.
    data_message_rx: UnboundedReceiver<InterProcessMessage>,
    /// MPSC channel to communicate messages to the [`DataPlane`] handler.
    data_plane_notification_tx: UnboundedSender<DataPlaneNotification>,
}

impl DataSender {
    pub(crate) fn new(
        worker_id: usize,
        tcp_stream: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
        data_message_rx: UnboundedReceiver<InterProcessMessage>,
        data_plane_notification_tx: UnboundedSender<DataPlaneNotification>,
    ) -> Self {
        Self {
            worker_id,
            tcp_stream,
            data_message_rx,
            data_plane_notification_tx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify the Worker that the DataSender is initialized.
        self.data_plane_notification_tx
            .send(DataPlaneNotification::SenderInitialized(self.worker_id))
            .map_err(CommunicationError::from)?;

        tracing::debug!("[DataSender {}] Initialized DataSender.", self.worker_id);

        // Listen for messages from different operators that must be forwarded on the TCP stream.
        loop {
            match self.data_message_rx.recv().await {
                Some(msg) => {
                    if let Err(e) = self
                        .tcp_stream
                        .send(msg)
                        .await
                        .map_err(CommunicationError::from)
                    {
                        return Err(e);
                    }
                }
                None => return Err(CommunicationError::Disconnected),
            }
        }
    }
}

/// Sends messages received from operator executors to other nodes.
/// The function launches a task for each TCP sink. Each task listens
/// on a mpsc channel for new `InterProcessMessages` messages, which it
/// forwards on the TCP stream.
pub(crate) async fn run_senders(senders: Vec<DataSender>) -> Result<(), CommunicationError> {
    // Waits until all futures complete. This code will only be reached
    // when all the mpsc channels are closed.
    future::join_all(
        senders
            .into_iter()
            .map(|mut sender| tokio::spawn(async move { sender.run().await })),
    )
    .await;
    Ok(())
}
