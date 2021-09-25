use async_trait::async_trait;
use serde::Deserialize;
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};

use crate::{
    communication::{Pusher, PusherT, RecvEndpoint, SendEndpoint},
    dataflow::{
        graph::{Job, JobGraph},
        stream::StreamId,
        Data, Message, ReadStream, WriteStream,
    },
    node::NodeId,
    scheduler::endpoints_manager::{ChannelsToReceivers, ChannelsToSenders},
    OperatorId,
};

#[async_trait]
pub trait StreamEndpointsT: Send {
    fn as_any(&mut self) -> &mut dyn Any;

    /// Creates a new inter-thread channel for the stream.
    ///
    /// It creates a `mpsc::Channel` and adds the sender and receiver to the
    /// corresponding endpoints.
    fn add_inter_thread_channel(&mut self);

    /// Adds a `SendEndpoint` to the other node.
    ///
    /// Assumes that `channels_to_senders` already stores a `mpsc::Sender` to the
    /// network sender to the other node.
    async fn add_inter_node_send_endpoint(
        &mut self,
        other_node_id: NodeId,
        channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
    ) -> Result<(), String>;
    fn add_inter_node_recv_endpoint(
        &mut self,
        receiver_pushers: &mut HashMap<StreamId, Box<dyn PusherT>>,
    ) -> Result<(), String>;
}

pub struct StreamEndpoints<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// The id of the stream.
    stream_id: StreamId,
    /// The receive endpoints of the stream.
    recv_endpoints: Vec<RecvEndpoint<Arc<Message<D>>>>,
    /// The send endpoints of the stream.
    send_endpoints: Vec<SendEndpoint<Arc<Message<D>>>>,
}

impl<D> StreamEndpoints<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            recv_endpoints: Vec::new(),
            send_endpoints: Vec::new(),
        }
    }

    /// Takes a `RecvEndpoint` out of the stream.
    fn take_recv_endpoint(&mut self) -> Result<RecvEndpoint<Arc<Message<D>>>, &'static str> {
        match self.recv_endpoints.pop() {
            Some(recv_endpoint) => Ok(recv_endpoint),
            None => Err("No more recv endpoints available"),
        }
    }

    /// Returns a cloned list of the `SendEndpoint`s the stream has.
    fn get_send_endpoints(&mut self) -> Result<Vec<SendEndpoint<Arc<Message<D>>>>, &'static str> {
        let mut result: Vec<SendEndpoint<Arc<Message<D>>>> = Vec::new();
        result.append(&mut self.send_endpoints);
        Ok(result)
    }

    fn add_send_endpoint(&mut self, endpoint: SendEndpoint<Arc<Message<D>>>) {
        self.send_endpoints.push(endpoint);
    }

    fn add_recv_endpoint(&mut self, endpoint: RecvEndpoint<Arc<Message<D>>>) {
        self.recv_endpoints.push(endpoint);
    }
}

#[async_trait]
impl<D> StreamEndpointsT for StreamEndpoints<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn add_inter_thread_channel(&mut self) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.add_send_endpoint(SendEndpoint::InterThread(tx));
        self.add_recv_endpoint(RecvEndpoint::InterThread(rx));
    }

    async fn add_inter_node_send_endpoint(
        &mut self,
        other_node_id: NodeId,
        channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
    ) -> Result<(), String> {
        let channels_to_senders = channels_to_senders.lock().await;
        if let Some(tx) = channels_to_senders.clone_channel(other_node_id) {
            self.add_send_endpoint(SendEndpoint::InterProcess(self.stream_id, tx));
            Ok(())
        } else {
            Err(format!("Unable to clone channel to node {}", other_node_id))
        }
    }

    fn add_inter_node_recv_endpoint(
        &mut self,
        receiver_pushers: &mut HashMap<StreamId, Box<dyn PusherT>>,
    ) -> Result<(), String> {
        let pusher: &mut Box<dyn PusherT> = receiver_pushers
            .entry(self.stream_id)
            .or_insert_with(|| Box::new(Pusher::<Arc<Message<D>>>::new()));
        if let Some(pusher) = pusher.as_any().downcast_mut::<Pusher<Arc<Message<D>>>>() {
            let (tx, rx) = mpsc::unbounded_channel();
            pusher.add_endpoint(SendEndpoint::InterThread(tx));
            self.add_recv_endpoint(RecvEndpoint::InterThread(rx));
            Ok(())
        } else {
            Err(format!(
                "Error casting pusher when adding inter node recv endpoint for stream {}",
                self.stream_id
            ))
        }
    }
}

/// Data structure that stores information needed to set up dataflow channels
/// by constructing individual transport channels.
pub(crate) struct ChannelManager {
    /// The node to which the [`ChannelManager`] belongs.
    node_id: NodeId,
    /// Stores a `StreamEndpoints` for each stream id.
    stream_entries: HashMap<StreamId, Box<dyn StreamEndpointsT>>,
}

#[allow(dead_code)]
impl ChannelManager {
    /// Creates transport channels between connected operators on this node, transport channels
    /// for operators with streams containing dataflow channels to other nodes, and transport
    /// channels from TCP receivers to operators that are connected to streams originating on
    /// other nodes.
    pub async fn new(
        job_graph: &JobGraph,
        node_id: NodeId,
        channels_to_receivers: Arc<Mutex<ChannelsToReceivers>>,
        channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
    ) -> Self {
        let mut channel_manager = Self {
            node_id,
            stream_entries: HashMap::new(),
        };

        let mut receiver_pushers: HashMap<StreamId, Box<dyn PusherT>> = HashMap::new();

        let local_operator_ids: Vec<OperatorId> = job_graph
            .get_operators()
            .into_iter()
            .filter(|o| o.config.node_id == node_id)
            .map(|o| o.id)
            .collect();

        let operators: HashMap<_, _> = job_graph
            .get_operators()
            .into_iter()
            .map(|o| (o.config.id, o))
            .collect();

        for (stream, source, destinations) in job_graph.get_streams() {
            // Whether the source is on the current node.
            let contains_source = match source {
                Job::Operator(operator_id) => local_operator_ids.contains(&operator_id),
                // TODO: change this when ERDOS programs are submitted to a cluster.
                Job::Driver => node_id == 0,
            };

            if contains_source {
                // The stream originates on this node.
                let stream_endpoint_t = channel_manager
                    .stream_entries
                    .entry(stream.id())
                    .or_insert_with(|| stream.to_stream_endpoints_t());

                for destination in destinations {
                    let destination_node_id = match destination {
                        Job::Operator(operator_id) => {
                            operators.get(&operator_id).unwrap().config.node_id
                        }
                        // TODO: change this when ERDOS programs are submitted to a cluster.
                        Job::Driver => 0,
                    };

                    if destination_node_id == node_id {
                        stream_endpoint_t.add_inter_thread_channel();
                    } else {
                        stream_endpoint_t
                            .add_inter_node_send_endpoint(
                                destination_node_id,
                                channels_to_senders.clone(),
                            )
                            .await
                            .unwrap();
                    }
                }
            } else {
                // The stream originates on another node.
                let contains_destination = destinations.iter().any(|destination| {
                    let destination_node_id = match destination {
                        Job::Operator(operator_id) => {
                            operators.get(&operator_id).unwrap().config.node_id
                        }
                        // TODO: change this when ERDOS programs are submitted to a cluster.
                        Job::Driver => 0,
                    };
                    node_id == destination_node_id
                });
                if contains_destination {
                    let stream_endpoint_t = channel_manager
                        .stream_entries
                        .entry(stream.id())
                        .or_insert_with(|| stream.to_stream_endpoints_t());
                    stream_endpoint_t
                        .add_inter_node_recv_endpoint(&mut receiver_pushers)
                        .unwrap();
                }
            }
        }

        // Send pushers to the DataReceiver which publishes received messages from TCP
        // on the proper transport channel.
        for (k, v) in receiver_pushers.into_iter() {
            channels_to_receivers.lock().await.send(k, v);
        }
        channel_manager
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Takes a `RecvEnvpoint` from a given stream.
    pub fn take_recv_endpoint<D>(
        &mut self,
        stream_id: StreamId,
    ) -> Result<RecvEndpoint<Arc<Message<D>>>, String>
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        if let Some(stream_entry_t) = self.stream_entries.get_mut(&stream_id) {
            if let Some(stream_entry) = stream_entry_t.as_any().downcast_mut::<StreamEndpoints<D>>()
            {
                match stream_entry.take_recv_endpoint() {
                    Ok(recv_endpoint) => Ok(recv_endpoint),
                    Err(msg) => Err(format!(
                        "Could not get recv endpoint with id {}: {}",
                        stream_id, msg
                    )),
                }
            } else {
                Err(format!(
                    "Type mismatch for recv endpoint with ID {}",
                    stream_id
                ))
            }
        } else {
            Err(format!("No recv endpoints found with ID {}", stream_id))
        }
    }

    /// Returns a cloned vector of the `SendEndpoint`s for a given stream.
    pub fn get_send_endpoints<D>(
        &mut self,
        stream_id: StreamId,
    ) -> Result<Vec<SendEndpoint<Arc<Message<D>>>>, String>
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        if let Some(stream_entry_t) = self.stream_entries.get_mut(&stream_id) {
            if let Some(stream_entry) = stream_entry_t.as_any().downcast_mut::<StreamEndpoints<D>>()
            {
                match stream_entry.get_send_endpoints() {
                    Ok(send_endpoints) => Ok(send_endpoints),
                    Err(msg) => Err(format!(
                        "Could not get recv endpoint with id {}: {}",
                        stream_id, msg
                    )),
                }
            } else {
                Err(format!(
                    "Type mismatch for recv endpoint with ID {}",
                    stream_id
                ))
            }
        } else {
            Err(format!("No recv endpoints found with ID {}", stream_id))
        }
    }

    /// This function can only be called once successfully.
    pub fn take_read_stream<D>(&mut self, stream_id: StreamId) -> Result<ReadStream<D>, String>
    where
        D: Data + for<'a> Deserialize<'a>,
    {
        self.take_recv_endpoint(stream_id)
            .map(|endpoint| ReadStream::new(stream_id, &stream_id.to_string(), Some(endpoint)))
    }

    pub fn get_write_stream<D>(&mut self, stream_id: StreamId) -> Result<WriteStream<D>, String>
    where
        D: Data + for<'a> Deserialize<'a>,
    {
        self.get_send_endpoints(stream_id)
            .map(|endpoints| WriteStream::from_endpoints(endpoints, stream_id))
    }
}
