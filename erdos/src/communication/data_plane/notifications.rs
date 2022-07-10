use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    communication::control_plane::notifications::WorkerAddress,
    dataflow::{
        graph::{AbstractStreamT, Job},
        stream::StreamId,
    },
    node::WorkerId,
};

use super::pusher::PusherT;

#[derive(Debug, Clone)]
pub(crate) enum StreamType {
    Read(Box<dyn AbstractStreamT>, WorkerAddress),
    Egress(Box<dyn AbstractStreamT>, WorkerAddress),
    Write(Box<dyn AbstractStreamT>, HashMap<Job, WorkerAddress>),
    Ingress(Box<dyn AbstractStreamT>, HashMap<Job, WorkerAddress>),
}

impl StreamType {
    pub(crate) fn id(&self) -> StreamId {
        match self {
            StreamType::Read(stream, _) => stream.id(),
            StreamType::Write(stream, _) => stream.id(),
            StreamType::Egress(stream, _) => stream.id(),
            StreamType::Ingress(stream, _) => stream.id(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DataPlaneNotification {
    SetupStreams(Job, Vec<StreamType>),
    StreamReady(Job, StreamId),
    ReceiverInitialized(WorkerId),
    SenderInitialized(WorkerId),
    InstallPusher(StreamId, Arc<Mutex<dyn PusherT>>),
    UpdatePusher(StreamId, Job),
    PusherUpdated(StreamId, Job),
}
