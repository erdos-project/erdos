use std::sync::{Arc, Mutex};

use futures::stream::{SplitSink, SplitStream};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::codec::Framed;

use crate::{
    communication::{CommunicationError, InterProcessMessage},
    dataflow::{graph::Job, stream::StreamId},
    node::WorkerId,
};

use super::{
    codec::MessageCodec, data_receiver::DataReceiver, data_sender::DataSender,
    notifications::DataPlaneNotification, pusher::PusherT,
};

pub struct WorkerConnection {
    worker_id: WorkerId,
    data_receiver_handle: JoinHandle<Result<(), CommunicationError>>,
    receiver_initialized: bool,
    data_sender_handle: JoinHandle<Result<(), CommunicationError>>,
    sender_initialized: bool,
    channel_to_data_receiver: UnboundedSender<DataPlaneNotification>,
    channel_to_data_sender: UnboundedSender<InterProcessMessage>,
}

impl WorkerConnection {
    pub(crate) fn new(
        worker_id: WorkerId,
        worker_sink: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
        worker_stream: SplitStream<Framed<TcpStream, MessageCodec>>,
        channel_to_data_plane: UnboundedSender<DataPlaneNotification>,
    ) -> Self {
        let (channel_to_data_receiver_tx, channel_to_data_receiver_rx) = mpsc::unbounded_channel();
        let mut data_receiver = DataReceiver::new(
            worker_id,
            worker_stream,
            channel_to_data_receiver_rx,
            channel_to_data_plane.clone(),
        );
        let data_receiver_handle = tokio::spawn(async move { data_receiver.run().await });

        let (channel_to_data_sender_tx, channel_to_data_sender_rx) = mpsc::unbounded_channel();
        let mut data_sender = DataSender::new(
            worker_id,
            worker_sink,
            channel_to_data_sender_rx,
            channel_to_data_plane,
        );
        let data_sender_handle = tokio::spawn(async move { data_sender.run().await });

        Self {
            worker_id,
            data_receiver_handle,
            receiver_initialized: false,
            data_sender_handle,
            sender_initialized: false,
            channel_to_data_receiver: channel_to_data_receiver_tx,
            channel_to_data_sender: channel_to_data_sender_tx,
        }
    }

    pub(crate) fn get_id(&self) -> WorkerId {
        self.worker_id
    }

    pub(crate) fn set_data_sender_initialized(&mut self) {
        self.sender_initialized = true;
    }

    pub(crate) fn set_data_receiver_initialized(&mut self) {
        self.receiver_initialized = true;
    }

    pub(crate) fn is_initialized(&mut self) -> bool {
        self.sender_initialized && self.receiver_initialized
    }

    pub(crate) fn install_pusher(
        &self,
        stream_id: StreamId,
        pusher: Arc<Mutex<dyn PusherT>>,
    ) -> Result<(), CommunicationError> {
        self.channel_to_data_receiver
            .send(DataPlaneNotification::InstallPusher(stream_id, pusher))
            .map_err(CommunicationError::from)
    }

    pub(crate) fn notify_pusher_update(
        &self,
        stream_id: StreamId,
        receiving_job: Job,
    ) -> Result<(), CommunicationError> {
        self.channel_to_data_receiver
            .send(DataPlaneNotification::UpdatePusher(
                stream_id,
                receiving_job,
            ))
            .map_err(CommunicationError::from)
    }

    pub(crate) fn get_channel_to_sender(&self) -> UnboundedSender<InterProcessMessage> {
        self.channel_to_data_sender.clone()
    }
}
