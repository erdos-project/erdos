use std::collections::HashMap;

use crate::{
    communication::{control_plane::notifications::WorkerAddress, PusherT},
    dataflow::{graph::AbstractOperator, stream::StreamId},
};

#[derive(Clone)]
pub(crate) enum DataPlaneNotification {
    SetupConnections(AbstractOperator, HashMap<StreamId, WorkerAddress>),
    ReceiverInitialized(usize),
    SenderInitialized(usize),
    PusherUpdate(StreamId, Box<dyn PusherT>),
    PusherUpdated(StreamId),
}
