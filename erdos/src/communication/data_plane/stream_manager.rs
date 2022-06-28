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
        graph::{AbstractStreamT, Job},
        stream::StreamId,
        Data, Message, ReadStream, WriteStream,
    },
    node::WorkerId,
};

pub(crate) trait StreamEndpointsT: Send {
    /// Upcasts the [`StreamEndpoints`] object to [`Any`].
    /// This is used to hide types in the [`StreamManager`] where it is
    /// downcast to the correct type upon request when retrieving the
    /// [`ReadStream`] or the [`WriteStream`].
    fn as_any(&mut self) -> &mut dyn Any;

    /// Retrieves the name of the Stream for which this trait object
    /// provides the endpoints for.
    fn name(&self) -> String;

    /// Retrieves the ID of the Stream for which this trait object
    /// provides the endpoints for.
    fn id(&self) -> StreamId;

    /// Adds a [`SendEndpoint`] and [`RecvEndpoint`] for the Stream for which
    /// this trait object provides the endpoints for.
    ///
    /// This creates an [`mpsc::unbounded_channel`] whose [`SendEndpoint`] is
    /// used to form the [`WriteStream`] for the source job, and the
    /// [`RecvEndpoint`] is used by the specified [`job`] to retrieve the
    /// messages from.
    fn add_intra_worker_channel(&mut self, job: Job);

    /// Adds a [`SendEndpoint`] for the Stream underlying this trait object to the
    /// specified [`job`].
    ///
    /// The [`channel_to_data_sender`] provides the channel to the [`DataSender`]
    /// running for the connection to the other [`Worker`], on which the SendEndpoint
    /// multiplexes the data messages on.
    fn add_inter_worker_send_endpoint(
        &mut self,
        job: Job,
        channel_to_data_sender: UnboundedSender<InterProcessMessage>,
    );

    /// Adds a [`RecvEndpoint`] for the given [`job`] on the Stream for which this
    /// trait object provides the endpoints for.
    fn add_inter_worker_recv_endpoint(&mut self, job: Job) -> Result<(), CommunicationError>;

    /// Clones the [`Pusher`] associated with this stream.
    ///
    /// Accessing the [`Pusher`] is only valid if there are some [`Job`]s on the current
    /// [`Worker`] that receive data from the stream underlying this trait object.
    ///
    /// The [`Pusher`] allows multiple [`Job`]s on the current [`Worker`] to register their
    /// receiving endpoints together on the same [`WorkerConnection`].
    fn clone_pusher(&self) -> Arc<Mutex<dyn PusherT>>;
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
    /// The [`Pusher`] for this particular stream, shared with the [`DataReceiver`]
    /// from where the [`Stream`] gets its data.
    pusher: Arc<Mutex<dyn PusherT>>,
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
            pusher: Arc::new(Mutex::new(Pusher::<Arc<Message<D>>>::new(stream_id))),
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

    fn id(&self) -> StreamId {
        self.stream_id
    }

    fn add_intra_worker_channel(&mut self, job: Job) {
        // Create a new MPSC unbounded channel, and save both the Send
        // and the Recv endpoints.
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

    fn add_inter_worker_recv_endpoint(&mut self, job: Job) -> Result<(), CommunicationError> {
        let recv_endpoint_from_pusher = {
            let mut pusher = self.pusher.lock().unwrap();
            if let Some(pusher) = pusher.as_any().downcast_mut::<Pusher<Arc<Message<D>>>>() {
                let (send_endpoint_to_pusher, recv_endpoint_from_pusher) =
                    mpsc::unbounded_channel();
                pusher.add_endpoint(job, SendEndpoint::InterThread(send_endpoint_to_pusher));
                Some(RecvEndpoint::InterThread(recv_endpoint_from_pusher))
            } else {
                None
            }
        };

        match recv_endpoint_from_pusher {
            Some(recv_endpoint_from_pusher) => {
                self.add_recv_endpoint(job, recv_endpoint_from_pusher);
                Ok(())
            }
            None => Err(CommunicationError::ProtocolError(format!(
                "Error casting Pusher when adding inter-worker \
                    receive endpoint for Stream {} (ID={}).",
                self.stream_name, self.stream_id,
            ))),
        }
    }

    fn clone_pusher(&self) -> Arc<Mutex<dyn PusherT>> {
        Arc::clone(&self.pusher)
    }
}

/// A [`StreamManager`] is a data structure that stores the constructed
/// endpoints for each stream. The [`DataPlane`] uses the [`StreamManager`]
/// to initiate both Inter-Worker and Intra-Worker connections for all
/// the [`Job`]s that are scheduled on the current [`Worker`]. Once the
/// connections are set up, the [`Operator`]s cast the [`StreamEndpoints`]
/// to the required message type and retrieve or send the data.
pub(crate) struct StreamManager {
    /// The [`Worker`] to which the [`StreamManager`] belongs.
    worker_id: WorkerId,
    /// Stores a `StreamEndpoints` for each stream id.
    stream_entries: HashMap<StreamId, Box<dyn StreamEndpointsT>>,
}

impl StreamManager {
    /// Initializes a new [`StreamManager`] for the [`Worker`] with the given ID.
    pub fn new(worker_id: WorkerId) -> Self {
        Self {
            worker_id,
            stream_entries: HashMap::new(),
        }
    }

    /// Retrieves the ID of the [`Worker`] for which this [`StreamManager`] was created.
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    /// Adds an Intra-Worker send and receive endpoint for a [`WriteStream`].
    /// The [`receiving_job`] specifies the [`Job`] that will construct a [`ReadStream`]
    /// from the corresponding [`RecvEndpoint`].
    pub fn add_intra_worker_endpoint(
        &mut self,
        stream: &Box<dyn AbstractStreamT>,
        receiving_job: Job,
    ) {
        // If there are no endpoints for this stream, create endpoints.
        let stream_endpoints = self
            .stream_entries
            .entry(stream.id())
            .or_insert_with(|| stream.to_stream_endpoints_t());

        // Register for a new intra-worker endpoint.
        stream_endpoints.add_intra_worker_channel(receiving_job);
    }

    /// Adds an Inter-Worker receipt endpoint for the [`stream`] to the [`receiving_job`]
    /// on the current [`Worker`] on the specified connection to another [`Worker`].
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
            let pusher = stream_endpoints.clone_pusher();
            self.stream_entries.insert(stream.id(), stream_endpoints);
            worker_connection.install_pusher(stream.id(), pusher)?;
        }

        // Register for a new endpoint with the Pusher.
        let stream_endpoints = self.stream_entries.get_mut(&stream.id()).unwrap();
        stream_endpoints.add_inter_worker_recv_endpoint(stream.get_source())?;
        worker_connection.notify_pusher_update(stream.get_source(), stream.id(), receiving_job)
    }

    /// Adds an Inter-Worker send endpoint for the [`stream`] to the [`destination_job`]
    /// from the current [`Worker`] on the specified connection to another [`Worker`].
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

        // Register for a new inter-worker endpoint.
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