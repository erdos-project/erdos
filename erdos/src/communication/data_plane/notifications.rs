use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    communication::{control_plane::notifications::WorkerAddress, PusherT},
    dataflow::{
        graph::{AbstractStreamT, Job},
        stream::StreamId,
    },
    node::WorkerId,
};

pub(crate) enum StreamType {
    ReadStream(Box<dyn AbstractStreamT>, WorkerAddress),
    WriteStream(Box<dyn AbstractStreamT>, HashMap<Job, WorkerAddress>),
}

#[derive(Debug, Clone)]
pub(crate) enum DataPlaneNotification {
    SetupReadStream(Box<dyn AbstractStreamT>, WorkerAddress),
    SetupWriteStream(Box<dyn AbstractStreamT>, HashMap<Job, WorkerAddress>),
    ReceiverInitialized(WorkerId),
    SenderInitialized(WorkerId),
    InstallPusher(StreamId, Arc<Mutex<dyn PusherT>>),
    UpdatePusher(StreamId, Job),
    PusherUpdated(StreamId, Job),
}
