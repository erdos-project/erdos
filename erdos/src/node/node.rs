use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    thread,
};

use futures_util::stream::StreamExt;
use tokio::{
    net::TcpStream,
    runtime::Builder,
    sync::{
        mpsc::{self, Receiver, Sender, UnboundedReceiver},
        Mutex,
    },
};
use tokio_util::codec::Framed;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::Configuration;
use crate::{
    communication::data_plane::StreamManager,
    dataflow::graph::{default_graph, JobGraph},
    node::WorkerId,
};
use crate::{
    communication::{self, ControlMessage, ControlMessageHandler, MessageCodec},
    dataflow::graph::AbstractGraph,
};

use super::worker::Worker;

/// Unique index for a [`Node`].
pub type NodeId = usize;

/// Structure which executes a portion of an ERDOS application.
///
/// The [`Node`] contains a runtime which executes operators and manages
/// communication between operators via streams.
#[allow(dead_code)]
pub struct Node {
    /// Node's configuration parameters.
    config: Configuration,
    /// Unique node id.
    id: NodeId,
    /// Queryable, uncompiled version of the JobGraph which stores metadata (e.g. stream names).
    abstract_graph: Option<AbstractGraph>,
    /// Dataflow graph which the node will execute.
    job_graph: Option<JobGraph>,
    // /// Structure to be used to send `Sender` updates to receiver threads.
    // channels_to_receivers: Arc<Mutex<ChannelsToReceivers>>,
    // /// Structure to be used to send messages to sender threads.
    // channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
    /// Structure used to send and receive control messages.
    control_handler: ControlMessageHandler,
    /// Used to block `run_async` until setup is complete for the driver to continue running safely.
    initialized: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    /// Channel used to shut down the node.
    shutdown_tx: Sender<()>,
    shutdown_rx: Option<Receiver<()>>,
    // Flushes buffered logs when dropped.
    logger_guard: Option<WorkerGuard>,
}

#[allow(dead_code)]
impl Node {
    /// Creates a new node.
    pub fn new(config: Configuration) -> Self {
        // Set up the logger.
        let logger_guard = if let Some(logging_level) = config.logging_level {
            let display_thread_ids = logging_level >= Level::TRACE;
            let display_target = logging_level >= Level::TRACE;

            let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());
            let subscriber = tracing_subscriber::fmt()
                .with_writer(non_blocking)
                .with_thread_ids(display_thread_ids)
                .with_span_events(FmtSpan::FULL)
                .with_target(display_target)
                .with_max_level(logging_level);
            subscriber.init();

            Some(guard)
        } else {
            None
        };

        let id = config.index;

        // Initialize ROS node.
        #[cfg(feature = "ros")]
        rosrust::init(&format!("erdos_node_{}", id));

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        Self {
            config,
            id,
            abstract_graph: None,
            job_graph: None,
            // channels_to_receivers: Arc::new(Mutex::new(ChannelsToReceivers::new())),
            // channels_to_senders: Arc::new(Mutex::new(ChannelsToSenders::new())),
            control_handler: ControlMessageHandler::new(),
            initialized: Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new())),
            shutdown_tx,
            shutdown_rx: Some(shutdown_rx),
            logger_guard,
        }
    }

    /// Runs an ERDOS node.
    ///
    /// The method never returns.
    pub fn run(&mut self) {
        tracing::debug!("Node {}: running", self.id);
        // Set the dataflow graph if it hasn't been set already.
        if self.job_graph.is_none() {
            let mut abstract_graph = default_graph::clone();
            self.job_graph = Some(abstract_graph.compile());
            self.abstract_graph = Some(abstract_graph);
        }
        // Build a runtime with n threads.
        let runtime = Builder::new_multi_thread()
            .worker_threads(self.config.num_threads)
            .thread_name(format!("node-{}", self.id))
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(self.async_run());
        tracing::debug!("Node {}: finished running", self.id);
    }

    /// Runs an ERDOS node in a seperate OS thread.
    ///
    /// The method immediately returns.
    pub fn run_async(mut self) -> NodeHandle {
        // Clone to avoid move to other thread.
        let shutdown_tx = self.shutdown_tx.clone();
        // Copy dataflow graph to the other thread
        let mut abstract_graph = default_graph::clone();
        self.job_graph = Some(abstract_graph.compile());
        self.abstract_graph = Some(abstract_graph);
        let initialized = self.initialized.clone();
        let thread_handle = thread::spawn(move || {
            self.run();
        });
        // Wait for ERDOS to start up.
        let (lock, cvar) = &*initialized;
        let mut started = lock.lock().unwrap();
        while !*started {
            started = cvar.wait(started).unwrap();
        }

        NodeHandle {
            thread_handle,
            shutdown_tx,
        }
    }

    fn set_node_initialized(&mut self) {
        let (lock, cvar) = &*self.initialized;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_all();

        tracing::debug!("Node {}: done initializing.", self.id);
    }

    /// Splits a vector of TCPStreams into `DataSender`s and `DataReceiver`s.
    // async fn split_data_streams(
    //     &mut self,
    //     streams: Vec<(NodeId, TcpStream)>,
    // ) -> (Vec<DataSender>, Vec<DataReceiver>) {
    //     let mut sink_halves = Vec::new();
    //     let mut stream_halves = Vec::new();

    //     // Create channels to the Sender and the Receiver
    //     // let mut data_receiver_control_rx = HashMap::with_capacity(streams.len());
    //     // let mut data_sender_control_rx = HashMap::with_capacity(streams.len());
    //     // {
    //     //     let mut channels_to_receivers = self.channels_to_receivers.lock().await;
    //     //     let mut channels_to_senders = self.channels_to_senders.lock().await;

    //     //     for (node_id, _) in streams.iter() {
    //     //         let (receiver_control_tx, receiver_control_rx) = mpsc::unbounded_channel();
    //     //         channels_to_receivers.add_sender(receiver_control_tx);
    //     //         data_receiver_control_rx.insert(*node_id, receiver_control_rx);

    //     //         let (sender_control_tx, sender_control_rx) = mpsc::unbounded_channel();
    //     //         channels_to_senders.add_sender(*node_id, sender_control_tx);
    //     //         data_sender_control_rx.insert(*node_id, sender_control_rx);
    //     //     }
    //     // }

    //     for (node_id, stream) in streams {
    //         // Use the message codec to divide the TCP stream data into messages.
    //         let framed = Framed::new(stream, MessageCodec::new());
    //         let (split_sink, split_stream) = framed.split();

    //         // Create an ERDOS receiver for the stream half.
    //         // stream_halves.push(
    //         //     DataReceiver::new(
    //         //         node_id,
    //         //         split_stream,
    //         //         data_receiver_control_rx.remove(&node_id).unwrap(),
    //         //         &mut self.control_handler,
    //         //     )
    //         //     .await,
    //         // );

    //         // Create an ERDOS sender for the sink half.
    //         // sink_halves.push(
    //         //     DataSender::new(
    //         //         node_id,
    //         //         split_sink,
    //         //         data_sender_control_rx.remove(&node_id).unwrap(),
    //         //         &mut self.control_handler,
    //         //     )
    //         //     .await,
    //         // );
    //     }
    //     (sink_halves, stream_halves)
    // }

    async fn wait_for_communication_layer_initialized(&mut self) -> Result<(), String> {
        // let num_nodes = self.config.data_addresses.len();

        // let mut data_senders_initialized = HashSet::new();
        // data_senders_initialized.insert(self.id);
        // let mut data_receivers_initialized = HashSet::new();
        // data_receivers_initialized.insert(self.id);

        // while data_senders_initialized.len() < num_nodes
        //     || data_receivers_initialized.len() < num_nodes
        // {
        //     let msg = self
        //         .control_handler
        //         .read_sender_or_receiver_initialized()
        //         .await
        //         .map_err(|e| format!("Error receiving control message: {:?}", e))?;
        //     match msg {
        //         ControlMessage::DataSenderInitialized(node_id) => {
        //             data_senders_initialized.insert(node_id);
        //         }
        //         ControlMessage::DataReceiverInitialized(node_id) => {
        //             data_receivers_initialized.insert(node_id);
        //         }
        //         _ => unreachable!(),
        //     };
        // }
        Ok(())
    }

    async fn wait_for_local_operators_initialized(
        &mut self,
        mut rx_from_operators: UnboundedReceiver<ControlMessage>,
        num_local_operators: usize,
    ) {
        let mut initialized_operators = HashSet::new();
        while initialized_operators.len() < num_local_operators {
            if let Some(ControlMessage::OperatorInitialized(op_id)) = rx_from_operators.recv().await
            {
                initialized_operators.insert(op_id);
            }
        }
    }

    async fn broadcast_local_operators_initialized(&mut self) -> Result<(), String> {
        tracing::debug!("Node {}: initialized all operators on this node.", self.id);
        self.control_handler
            .broadcast_to_nodes(ControlMessage::AllOperatorsInitializedOnNode(self.id))
            .map_err(|e| format!("Error broadcasting control message: {:?}", e))
    }

    async fn wait_for_all_operators_initialized(&mut self) -> Result<(), String> {
        // let num_nodes = self.config.data_addresses.len();
        // let mut initialized_nodes = HashSet::new();
        // initialized_nodes.insert(self.id);
        // while initialized_nodes.len() < num_nodes {
        //     match self
        //         .control_handler
        //         .read_all_operators_initialized_on_node_msg()
        //         .await
        //     {
        //         Ok(node_id) => {
        //             initialized_nodes.insert(node_id);
        //         }
        //         Err(e) => {
        //             return Err(format!("Error waiting for other nodes to set up: {:?}", e));
        //         }
        //     }
        // }
        Ok(())
    }

    async fn run_operators(&mut self) -> Result<(), String> {
        self.wait_for_communication_layer_initialized().await?;

        let job_graph = self
            .job_graph
            .as_ref()
            .unwrap_or_else(|| panic!("Node {}: dataflow graph must be set.", self.id));

        if let Some(filename) = &self.config.graph_filename {
            job_graph
                .to_graph_viz(filename.as_str())
                .map_err(|e| e.to_string())?;
        }

        let channel_manager = StreamManager::new(WorkerId::nil());
        // Execute operators scheduled on the current node.
        let channel_manager = Arc::new(std::sync::Mutex::new(channel_manager));
        let num_operators = job_graph.operators().len();
        tracing::debug!("There are {} operators total", num_operators);
        let local_operators: Vec<_> = job_graph
            .operators()
            .into_iter()
            .filter(|op| op.config.node_id == self.id)
            .collect();

        let num_local_operators = local_operators.len();
        tracing::debug!("{} local operators", num_local_operators);

        // TODO: choose a better value.
        let num_event_runners = std::cmp::max(
            self.config
                .num_threads
                .checked_sub(num_local_operators)
                .unwrap_or(1),
            num_local_operators,
        );
        let mut worker = Worker::new(num_event_runners);

        let mut operator_executors = Vec::with_capacity(num_local_operators);

        for operator_info in local_operators {
            let name = operator_info
                .config
                .name
                .clone()
                .unwrap_or_else(|| format!("{}", operator_info.id));
            tracing::debug!("Node {}: starting operator {}", self.id, name);
            let channel_manager_copy = Arc::clone(&channel_manager);
            // Launch the operator as a separate async task.
            match job_graph.get_operator_runner(&operator_info.id) {
                Some(operator_runner) => {
                    let operator_executor = (operator_runner)(channel_manager_copy);
                    operator_executors.push(operator_executor);
                }
                None => {
                    tracing::error!(
                        "Node {}: Could not find an executor for operator {}",
                        self.id,
                        name
                    );
                }
            }
        }

        worker.spawn_tasks(operator_executors).await;
        // TODO: Wait for all operators to finish setting up.

        // Setup driver on the current node.
        if self.id == 0 {
            let mut channel_manager_mut = channel_manager.lock().unwrap();
            for setup_hook in job_graph.get_driver_setup_hooks() {
                (setup_hook)(
                    self.abstract_graph.as_ref().unwrap(),
                    &mut channel_manager_mut,
                );
            }
        }
        // Broadcast all operators initialized on current node.
        self.broadcast_local_operators_initialized().await?;
        // Wait for all other nodes to finish setting up.
        self.wait_for_all_operators_initialized().await?;
        // Tell driver to run.
        self.set_node_initialized();
        // TODO: Tell all operators to run.
        // Wait for all operators to finish running.
        worker.execute().await;
        Ok(())
    }

    async fn async_run(&mut self) {
        // Assign values used later to avoid lifetime errors.
        // let num_nodes = self.config.data_addresses.len();
        // Create TCPStreams between all node pairs.
        // let data_streams =
        //     communication::create_tcp_streams(self.config.data_addresses.clone(), self.id).await;
        // let (senders, receivers) = self.split_data_streams(data_streams).await;
        // Listen for shutdown message.
        let mut shutdown_rx = self.shutdown_rx.take().unwrap();
        let shutdown_fut = shutdown_rx.recv();
        // Execute threads that send data to other nodes.
        // let senders_fut = senders::run_senders(senders);
        // Execute threads that receive data from other nodes.
        // let recvs_fut = receivers::run_receivers(receivers);
        // Execute operators.
        let ops_fut = self.run_operators();
        // These threads only complete when a failure happens.
        // if num_nodes <= 1 {
        if true {
            // Senders and Receivers should return if there's only 1 node.
            // if let Err(e) = tokio::try_join!(senders_fut, recvs_fut,) {
            //     tracing::error!(
            //         "Non-fatal network communication error; this should not happen! {:?}",
            //         e
            //     );
            // }
            tokio::select! {
                Err(e) = ops_fut => tracing::error!(

                    "Error running operators on node {:?}: {:?}", self.id, e
                ),
                _ = shutdown_fut => tracing::debug!("Node {}: shutting down", self.id),
            }
        } else {
            tokio::select! {
                // Err(e) = senders_fut => tracing::error!("Error with data senders: {:?}", e),
                // Err(e) = recvs_fut => tracing::error!("Error with data receivers: {:?}", e),
                Err(e) = ops_fut => tracing::error!(

                    "Error running operators on node {:?}: {:?}", self.id, e
                ),
                _ = shutdown_fut => tracing::debug!("Node {}: shutting down", self.id),
            }
        }
    }
}

/// Handle to a [`Node`] running asynchronously.
pub struct NodeHandle {
    thread_handle: thread::JoinHandle<()>,
    shutdown_tx: Sender<()>,
}

// TODO: distinguish between shutting down the dataflow and shutting down the node.
impl NodeHandle {
    /// Waits for the associated [`Node`] to finish.
    pub fn join(self) -> Result<(), String> {
        self.thread_handle.join().map_err(|e| format!("{:?}", e))
    }
    /// Blocks until the [`Node`] shuts down.
    pub fn shutdown(self) -> Result<(), String> {
        // Error indicates node is already shutting down.
        self.shutdown_tx.try_send(()).ok();
        self.thread_handle.join().map_err(|e| format!("{:?}", e))
    }
}
