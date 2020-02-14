use futures_util::stream::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    thread,
};
use tokio::{net::TcpStream, runtime::Builder, sync::Mutex};
use tokio_util::codec::Framed;

use crate::communication::{
    self,
    receivers::{self, ControlReceiver, DataReceiver},
    senders::{self, ControlSender, DataSender},
    ControlMessage, ControlMessageCodec, ControlMessageHandler, MessageCodec,
};
use crate::dataflow::graph::default_graph;
use crate::scheduler::{
    self,
    channel_manager::ChannelManager,
    endpoints_manager::{ChannelsToReceivers, ChannelsToSenders},
};
use crate::Configuration;

pub type NodeId = usize;

#[allow(dead_code)]
pub struct Node {
    /// Node's configuration parameters.
    config: Configuration,
    /// Unique node id.
    id: NodeId,
    /// Structure to be used to send `Sender` updates to receiver threads.
    channels_to_receivers: Arc<Mutex<ChannelsToReceivers>>,
    /// Structure to be used to send messages to sender threads.
    channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
    /// Structure used to send and receive control messages.
    control_handler: ControlMessageHandler,
    /// Used to block `run_async` until setup is complete for the driver to continue running safely.
    initialized: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl Node {
    /// Creates a new node.
    pub fn new(config: Configuration) -> Self {
        let id = config.index;
        let logger = config.logger.clone();
        Self {
            config,
            id,
            channels_to_receivers: Arc::new(Mutex::new(ChannelsToReceivers::new())),
            channels_to_senders: Arc::new(Mutex::new(ChannelsToSenders::new())),
            control_handler: ControlMessageHandler::new(logger),
            initialized: Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new())),
        }
    }

    /// Runs an ERDOS node.
    ///
    /// The method never returns.
    pub fn run(&mut self) {
        debug!(self.config.logger, "Starting node {}", self.id);
        // Build a runtime with n threads.
        let mut runtime = Builder::new()
            .threaded_scheduler()
            .thread_name(format!("node-{}", self.id))
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(self.async_run());
    }

    /// Runs an ERDOS node in a seperate OS thread.
    ///
    /// The method immediately returns.
    pub fn run_async(mut self) {
        // Copy dataflow graph to the other thread
        let graph = default_graph::clone();
        let initialized = self.initialized.clone();
        thread::spawn(move || {
            default_graph::set(graph);
            self.run();
        });
        // Wait for ERDOS to start up.
        let (lock, cvar) = &*initialized;
        let mut started = lock.lock().unwrap();
        while !*started {
            started = cvar.wait(started).unwrap();
        }
    }

    fn set_node_initialized(&mut self) {
        let (lock, cvar) = &*self.initialized;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_all();

        debug!(self.config.logger, "Notfiying node initialized");
    }

    /// Splits a vector of TCPStreams into `DataSender`s and `DataReceiver`s.
    async fn split_data_streams(
        &mut self,
        mut streams: Vec<(NodeId, TcpStream)>,
    ) -> (Vec<DataSender>, Vec<DataReceiver>) {
        let mut sink_halves = Vec::new();
        let mut stream_halves = Vec::new();
        while let Some((node_id, stream)) = streams.pop() {
            // Use the message codec to divide the TCP stream data into messages.
            let framed = Framed::new(stream, MessageCodec::new());
            let (split_sink, split_stream) = framed.split();
            // Create an ERDOS receiver for the stream half.
            stream_halves.push(
                DataReceiver::new(
                    node_id,
                    split_stream,
                    self.channels_to_receivers.clone(),
                    &mut self.control_handler,
                )
                .await,
            );

            // Create an ERDOS sender for the sink half.
            sink_halves.push(
                DataSender::new(
                    node_id,
                    split_sink,
                    self.channels_to_senders.clone(),
                    &mut self.control_handler,
                )
                .await,
            );
        }
        (sink_halves, stream_halves)
    }

    /// Splits a vector of TCPStreams into `ControlMessageHandler`, `ControlSender`s and `ControlReceiver`s.
    async fn split_control_streams(
        &mut self,
        streams: Vec<(NodeId, TcpStream)>,
    ) -> (Vec<ControlSender>, Vec<ControlReceiver>) {
        let mut control_receivers = Vec::new();
        let mut control_senders = Vec::new();

        for (node_id, stream) in streams {
            // Use the message codec to divide the TCP stream data into messages.
            let framed = Framed::new(stream, ControlMessageCodec::new());
            let (split_sink, split_stream) = framed.split();
            // Create an control receiver for the stream half.
            control_receivers.push(ControlReceiver::new(
                node_id,
                split_stream,
                &mut self.control_handler,
            ));
            // Create an control sender for the sink half.
            control_senders.push(ControlSender::new(
                node_id,
                split_sink,
                &mut self.control_handler,
            ));
        }

        (control_senders, control_receivers)
    }

    async fn wait_for_communication_layer_initialized(&mut self) -> Result<(), String> {
        let num_nodes = self.config.data_addresses.len();

        let mut control_senders_initialized = HashSet::new();
        control_senders_initialized.insert(self.id);
        let mut control_receivers_initialized = HashSet::new();
        control_receivers_initialized.insert(self.id);
        let mut data_senders_initialized = HashSet::new();
        data_senders_initialized.insert(self.id);
        let mut data_receivers_initialized = HashSet::new();
        data_receivers_initialized.insert(self.id);

        while control_senders_initialized.len() < num_nodes
            || control_receivers_initialized.len() < num_nodes
            || data_senders_initialized.len() < num_nodes
            || data_receivers_initialized.len() < num_nodes
        {
            let msg = self
                .control_handler
                .read_sender_or_receiver_initialized()
                .await
                .map_err(|e| format!("Error receiving control message: {:?}", e))?;
            match msg {
                ControlMessage::ControlSenderInitialized(node_id) => {
                    control_senders_initialized.insert(node_id);
                }
                ControlMessage::ControlReceiverInitialized(node_id) => {
                    control_receivers_initialized.insert(node_id);
                }
                ControlMessage::DataSenderInitialized(node_id) => {
                    data_senders_initialized.insert(node_id);
                }
                ControlMessage::DataReceiverInitialized(node_id) => {
                    data_receivers_initialized.insert(node_id);
                }
                _ => unreachable!(),
            };
        }
        Ok(())
    }

    async fn wait_for_local_operators_initialized(
        &mut self,
        rx_from_operators: std::sync::mpsc::Receiver<ControlMessage>,
        num_local_operators: usize,
    ) {
        let mut initialized_operators = HashSet::new();
        while initialized_operators.len() < num_local_operators {
            if let Ok(ControlMessage::OperatorInitialized(op_id)) = rx_from_operators.recv() {
                initialized_operators.insert(op_id);
            }
        }
    }

    async fn broadcast_local_operators_initialized(&mut self) -> Result<(), String> {
        self.control_handler
            .broadcast_to_nodes(ControlMessage::AllOperatorsInitializedOnNode(self.id))
            .map_err(|e| format!("Error broadcasting control message: {:?}", e))
    }

    async fn wait_for_all_operators_initialized(&mut self) -> Result<(), String> {
        let num_nodes = self.config.data_addresses.len();
        let mut initialized_nodes = HashSet::new();
        initialized_nodes.insert(self.id);
        while initialized_nodes.len() < num_nodes {
            match self
                .control_handler
                .read_all_operators_initialized_on_node_msg()
                .await
            {
                Ok(node_id) => {
                    initialized_nodes.insert(node_id);
                }
                Err(e) => {
                    return Err(format!("Error waiting for other nodes to set up: {:?}", e));
                }
            }
        }
        Ok(())
    }

    async fn run_operators(&mut self) -> Result<(), String> {
        self.wait_for_communication_layer_initialized().await?;

        let graph = scheduler::schedule(&default_graph::clone());

        let channel_manager = ChannelManager::new(
            &graph,
            self.id,
            Arc::clone(&self.channels_to_receivers),
            Arc::clone(&self.channels_to_senders),
        )
        .await;
        // Execute operators scheduled on the current node.
        let channel_manager = Arc::new(std::sync::Mutex::new(channel_manager));
        let local_operators: Vec<_> = graph
            .get_operators()
            .into_iter()
            .filter(|op| op.node_id == self.id)
            .collect();

        let (operator_tx, rx_from_operators) = std::sync::mpsc::channel();
        let mut channels_to_operators = HashMap::new();

        let num_local_operators = local_operators.len();

        for operator_info in local_operators {
            debug!(
                self.config.logger,
                "Executing operator {} on node {}", operator_info.id, operator_info.node_id
            );
            let channel_manager_copy = Arc::clone(&channel_manager);
            let operator_tx_copy = operator_tx.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            channels_to_operators.insert(operator_info.id, tx);
            // Launch the operator as a separate async task.
            tokio::spawn(async move {
                (operator_info.runner)(channel_manager_copy, operator_tx_copy, rx).execute();
            });
        }

        // Wait for all operators to finish setting up.
        self.wait_for_local_operators_initialized(rx_from_operators, num_local_operators)
            .await;
        // Broadcast all operators initialized on current node.
        self.broadcast_local_operators_initialized().await?;
        // Wait for all other nodes to finish setting up.
        self.wait_for_all_operators_initialized().await?;
        // Setup driver on the current node.
        if let Some(driver) = graph.get_driver(self.id) {
            for setup_hook in driver.setup_hooks {
                (setup_hook)(Arc::clone(&channel_manager));
            }
        }
        // Tell driver to run.
        self.set_node_initialized();
        // Tell all operators to run.
        for (op_id, tx) in channels_to_operators {
            tx.send(ControlMessage::RunOperator(op_id))
                .map_err(|e| format!("Error telling operator to run: {}", e))?;
        }

        Ok(())
    }

    async fn async_run(&mut self) {
        // Assign values used later to avoid lifetime errors.
        let num_nodes = self.config.data_addresses.len();
        let logger = self.config.logger.clone();
        // Create TCPStreams between all node pairs.
        let control_streams = communication::create_tcp_streams(
            self.config.control_addresses.clone(),
            self.id,
            &self.config.logger,
        )
        .await;
        let data_streams = communication::create_tcp_streams(
            self.config.data_addresses.clone(),
            self.id,
            &self.config.logger,
        )
        .await;
        let (control_senders, control_receivers) =
            self.split_control_streams(control_streams).await;
        let (senders, receivers) = self.split_data_streams(data_streams).await;
        // Execute threads that send data to other nodes.
        let control_senders_fut = senders::run_control_senders(control_senders);
        let senders_fut = senders::run_senders(senders);
        // Execute threads that receive data from other nodes.
        let control_recvs_fut = receivers::run_control_receivers(control_receivers);
        let recvs_fut = receivers::run_receivers(receivers);
        // Execute operators.
        let ops_fut = self.run_operators();
        // These threads only complete when a failure happens.
        if num_nodes <= 1 {
            // Senders and Receivers should return if there's only 1 node.
            if let Err(e) = tokio::try_join!(
                senders_fut,
                recvs_fut,
                control_senders_fut,
                control_recvs_fut
            ) {
                error!(
                    logger,
                    "Non-fatal network communication error; this should not happen! {:?}", e
                );
            }
            if let Err(e) = ops_fut.await {
                error!(
                    logger,
                    "Error running operators on node {:?}: {:?}", self.id, e
                );
            }
        } else {
            tokio::select! {
                Err(e) = senders_fut => error!(logger, "Error with data senders: {:?}", e),
                Err(e) = recvs_fut => error!(logger, "Error with data receivers: {:?}", e),
                Err(e) = control_senders_fut => error!(logger, "Error with control senders: {:?}", e),
                Err(e) = control_recvs_fut => error!(
                    self.config.logger,
                    "Error with control receivers: {:?}", e
                ),
                Err(e) = ops_fut => error!(
                    logger,
                    "Error running operators on node {:?}: {:?}", self.id, e
                ),
            }
        }
    }
}
