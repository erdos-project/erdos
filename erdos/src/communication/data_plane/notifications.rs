use std::collections::HashMap;

use crate::{
    communication::control_plane::notifications::WorkerAddress,
    dataflow::{graph::AbstractOperator, stream::StreamId},
};

pub(crate) enum DataPlaneNotification {
    SetupConnections(AbstractOperator, HashMap<StreamId, WorkerAddress>),
}
