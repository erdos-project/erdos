use std::collections::HashMap;

use crate::{
    communication::{control_plane::notifications::WorkerAddress, PusherT},
    dataflow::{graph::AbstractOperator, stream::StreamId}, node::WorkerId,
};

#[derive(Clone)]
pub(crate) enum DataPlaneNotification {
    SetupConnections(AbstractOperator, HashMap<StreamId, WorkerAddress>),
    ReceiverInitialized(WorkerId),
    SenderInitialized(WorkerId),
    PusherUpdate(StreamId, Box<dyn PusherT>),
    PusherUpdated(StreamId),
}
