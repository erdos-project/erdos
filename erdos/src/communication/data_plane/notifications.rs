use std::collections::HashMap;

use crate::{
    communication::{control_plane::notifications::WorkerAddress, PusherT},
    dataflow::{graph::AbstractStreamT, stream::StreamId},
    node::WorkerId,
};

#[derive(Clone)]
pub(crate) enum DataPlaneNotification {
    SetupReadStream(Box<dyn AbstractStreamT>, WorkerAddress),
    SetupWriteStream(Box<dyn AbstractStreamT>, HashMap<StreamId, WorkerAddress>),
    ReceiverInitialized(WorkerId),
    SenderInitialized(WorkerId),
    PusherUpdate(StreamId, Box<dyn PusherT>),
    PusherUpdated(StreamId),
}
