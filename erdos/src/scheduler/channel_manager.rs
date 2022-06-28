use async_trait::async_trait;
use serde::Deserialize;
use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::{
    communication::{
        data_plane::worker_connection::WorkerConnection, CommunicationError, InterProcessMessage,
        Pusher, PusherT, RecvEndpoint, SendEndpoint,
    },
    dataflow::{
        graph::{AbstractStreamT, Job, JobGraph},
        stream::StreamId,
        Data, Message, ReadStream, WriteStream,
    },
    node::NodeId,
    OperatorId,
};

#[async_trait]
pub(crate) trait StreamEndpointsT: Send {
    fn as_any(&mut self) -> &mut dyn Any;

    fn name(&self) -> String;

    /// Creates a new inter-thread channel for the stream.
    ///
    /// It creates a `mpsc::Channel` and adds the sender and receiver to the
    /// corresponding endpoints.
    fn add_inter_thread_channel(&mut self, job: Job);

    /// Adds a `SendEndpoint` to the other node.
    ///
    /// Assumes that `channels_to_senders` already stores a `mpsc::Sender` to the
    /// network sender to the other node.
    fn add_inter_worker_send_endpoint(
        &mut self,
        job: Job,
        channel_to_data_sender: UnboundedSender<InterProcessMessage>,
    );

    fn add_inter_worker_recv_endpoint(
        &mut self,
        job: Job,
        pusher: Arc<Mutex<dyn PusherT>>,
    ) -> Result<(), String>;

    fn get_pusher(&self) -> Arc<Mutex<dyn PusherT>>;
}

pub struct StreamEndpoints<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// The id of the stream.
    stream_id: StreamId,
    /// The name of the stream.
    stream_name: String,
    /// The receive endpoints of the stream.
    recv_endpoints: HashMap<Job, RecvEndpoint<Arc<Message<D>>>>,
    /// The send endpoints of the stream.
    send_endpoints: HashMap<Job, SendEndpoint<Arc<Message<D>>>>,
}

impl<D> StreamEndpoints<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub fn new(stream_id: StreamId, stream_name: String) -> Self {
        Self {
            stream_id,
            stream_name,
            recv_endpoints: HashMap::new(),
            send_endpoints: HashMap::new(),
        }
    }

    /// Takes a `RecvEndpoint` out of the stream.
    fn take_recv_endpoint(&mut self) -> Result<RecvEndpoint<Arc<Message<D>>>, &'static str> {
        let key = self.recv_endpoints.keys().cloned().next();
        match key {
            Some(job) => Ok(self.recv_endpoints.remove(&job).unwrap()),
            None => Err("No more recv endpoints available"),
        }
    }

    /// Returns a cloned list of the `SendEndpoint`s the stream has.
    fn get_send_endpoints(&mut self) -> HashMap<Job, SendEndpoint<Arc<Message<D>>>> {
        self.send_endpoints.clone()
    }

    fn add_send_endpoint(&mut self, job: Job, endpoint: SendEndpoint<Arc<Message<D>>>) {
        self.send_endpoints.insert(job, endpoint);
    }

    fn add_recv_endpoint(&mut self, job: Job, endpoint: RecvEndpoint<Arc<Message<D>>>) {
        self.recv_endpoints.insert(job, endpoint);
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

    fn name(&self) -> String {
        self.stream_name.clone()
    }

    fn add_inter_thread_channel(&mut self, job: Job) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.add_send_endpoint(job, SendEndpoint::InterThread(tx));
        self.add_recv_endpoint(job, RecvEndpoint::InterThread(rx));
    }

    fn add_inter_worker_send_endpoint(
        &mut self,
        job: Job,
        channel_to_data_sender: UnboundedSender<InterProcessMessage>,
    ) {
        self.add_send_endpoint(
            job,
            SendEndpoint::InterProcess(self.stream_id, channel_to_data_sender),
        );
    }

    fn add_inter_worker_recv_endpoint(
        &mut self,
        job: Job,
        pusher: Arc<Mutex<dyn PusherT>>,
    ) -> Result<(), String> {
        let mut pusher = pusher.lock().unwrap();
        if let Some(pusher) = pusher.as_any().downcast_mut::<Pusher<Arc<Message<D>>>>() {
            let (tx, rx) = mpsc::unbounded_channel();
            pusher.add_endpoint(job, SendEndpoint::InterThread(tx));
            self.add_recv_endpoint(job, RecvEndpoint::InterThread(rx));
            Ok(())
        } else {
            Err(format!(
                "Error casting pusher when adding inter node recv endpoint for stream {}",
                self.stream_id
            ))
        }
    }

    fn get_pusher(&self) -> Arc<Mutex<dyn PusherT>> {
        Arc::new(Mutex::new(Pusher::<Arc<Message<D>>>::new(self.stream_id)))
    }
}

/// Data structure that stores information needed to set up dataflow channels
/// by constructing individual transport channels.
pub(crate) struct StreamManager {
    /// The node to which the [`ChannelManager`] belongs.
    node_id: NodeId,
    /// Stores a `StreamEndpoints` for each stream id.
    stream_entries: HashMap<StreamId, Box<dyn StreamEndpointsT>>,
    stream_pushers: HashMap<StreamId, Arc<Mutex<dyn PusherT>>>,
}

#[allow(dead_code)]
impl StreamManager {
    /// Creates transport channels between connected operators on this node, transport channels
    /// for operators with streams containing dataflow channels to other nodes, and transport
    /// channels from TCP receivers to operators that are connected to streams originating on
    /// other nodes.
    #[allow(clippy::needless_collect)]
    pub async fn new(job_graph: &JobGraph, node_id: NodeId) -> Self {
        let mut channel_manager = Self {
            node_id,
            stream_entries: HashMap::new(),
            stream_pushers: HashMap::new(),
        };

        let mut receiver_pushers: HashMap<StreamId, Box<dyn PusherT>> = HashMap::new();

        let local_operator_ids: Vec<OperatorId> = job_graph
            .operators()
            .into_iter()
            .filter(|o| o.config.node_id == node_id)
            .map(|o| o.id)
            .collect();

        let operators: HashMap<_, _> = job_graph
            .operators()
            .into_iter()
            .map(|o| (o.config.id, o))
            .collect();

        for stream in job_graph.get_streams() {
            let source = stream.get_source();
            let destinations = stream.get_destinations();

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

                // Stores the number of jobs on each connected node.
                let mut destination_nodes: HashMap<NodeId, usize> = HashMap::new();
                for destination in destinations {
                    let destination_node_id = match destination {
                        Job::Operator(operator_id) => {
                            operators.get(&operator_id).unwrap().config.node_id
                        }
                        // TODO: change this when ERDOS programs are submitted to a cluster.
                        Job::Driver => 0,
                    };

                    let entry = destination_nodes.entry(destination_node_id).or_default();
                    *entry += 1;
                }

                for (destination_node_id, count) in destination_nodes.into_iter() {
                    if destination_node_id == node_id {
                        for _ in 0..count {
                            // stream_endpoint_t.add_inter_thread_channel();
                        }
                    } else {
                        todo!()
                        // stream_endpoint_t
                        //     .add_inter_node_send_endpoint(
                        //         destination_node_id,
                        //         channels_to_senders.clone(),
                        //     )
                        //     .await
                        //     .unwrap();
                    }
                }
            } else {
                // The stream originates on another node.
                let num_local_destinations = destinations
                    .iter()
                    .filter(|destination| {
                        let destination_node_id = match destination {
                            Job::Operator(operator_id) => {
                                operators.get(operator_id).unwrap().config.node_id
                            }
                            // TODO: change this when ERDOS programs are submitted to a cluster.
                            Job::Driver => 0,
                        };
                        node_id == destination_node_id
                    })
                    .count();
                for _ in 0..num_local_destinations {
                    let stream_endpoint_t = channel_manager
                        .stream_entries
                        .entry(stream.id())
                        .or_insert_with(|| stream.to_stream_endpoints_t());
                    // stream_endpoint_t
                    //     .add_inter_node_recv_endpoint(&mut receiver_pushers)
                    //     .unwrap();
                }
            }
        }

        // Send pushers to the DataReceiver which publishes received messages from TCP
        // on the proper transport channel.
        for (k, v) in receiver_pushers.into_iter() {
            todo!()
            // channels_to_receivers.lock().await.send(k, v);
        }
        channel_manager
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn add_inter_worker_recv_endpoint(
        &mut self,
        stream: &Box<dyn AbstractStreamT>,
        receiving_job: Job,
        worker_connection: &WorkerConnection,
    ) -> Result<(), CommunicationError> {
        // If there are no endpoints for this stream, create endpoints and install
        // the pusher to the DataReceiver at this connection.
        if !self.stream_entries.contains_key(&stream.id()) {
            let stream_endpoints = stream.to_stream_endpoints_t();
            let pusher = stream_endpoints.get_pusher();
            self.stream_entries.insert(stream.id(), stream_endpoints);
            self.stream_pushers.insert(stream.id(), Arc::clone(&pusher));
            worker_connection.install_pusher(stream.id(), pusher)?;
        }

        // Register for a new endpoint with the Pusher.
        let stream_endpoints = self.stream_entries.get_mut(&stream.id()).unwrap();
        let stream_pusher = self.stream_pushers.get(&stream.id()).unwrap();
        let _ = stream_endpoints
            .add_inter_worker_recv_endpoint(stream.get_source(), Arc::clone(stream_pusher));
        worker_connection.notify_pusher_update(stream.get_source(), stream.id(), receiving_job)?;

        Ok(())
    }

    pub fn add_inter_worker_send_endpoint(
        &mut self,
        stream: &Box<dyn AbstractStreamT>,
        destination_job: Job,
        worker_connection: &WorkerConnection,
    ) {
        // If there are no endpoints for this stream, create endpoints.
        let stream_endpoints = self
            .stream_entries
            .entry(stream.id())
            .or_insert_with(|| stream.to_stream_endpoints_t());

        // Register for a new endpoint.
        stream_endpoints.add_inter_worker_send_endpoint(
            destination_job,
            worker_connection.get_channel_to_sender(),
        )
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
    ) -> Result<HashMap<Job, SendEndpoint<Arc<Message<D>>>>, String>
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        if let Some(stream_entry_t) = self.stream_entries.get_mut(&stream_id) {
            if let Some(stream_entry) = stream_entry_t.as_any().downcast_mut::<StreamEndpoints<D>>()
            {
                Ok(stream_entry.get_send_endpoints())
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
            .map(|endpoint| ReadStream::new(stream_id, &stream_id.to_string(), endpoint))
    }

    pub fn write_stream<D>(&mut self, stream_id: StreamId) -> Result<WriteStream<D>, String>
    where
        D: Data + for<'a> Deserialize<'a>,
    {
        let name = self
            .stream_entries
            .get(&stream_id)
            .ok_or_else(|| format!("Could not find stream with ID {}", stream_id))?
            .name();
        self.get_send_endpoints(stream_id)
            .map(|endpoints| WriteStream::new(stream_id, &name, endpoints))
    }
}

impl Default for StreamManager {
    fn default() -> Self {
        Self {
            node_id: 0,
            stream_entries: HashMap::new(),
            stream_pushers: HashMap::new(),
        }
    }
}
