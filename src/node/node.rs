use futures::future;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    thread,
    time::Duration,
};
use tokio::{codec::Framed, net::TcpStream, prelude::*, runtime::Builder, sync::Mutex};

use crate::communication::{
    self,
    receivers::{self, ControlReceiver, ERDOSReceiver},
    senders::{self, ControlSender, ERDOSSender},
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
}

impl Node {
    /// Creates a new node.
    pub fn new(config: Configuration) -> Self {
        let id = config.index;
        Self {
            config,
            id,
            channels_to_receivers: Arc::new(Mutex::new(ChannelsToReceivers::new())),
            channels_to_senders: Arc::new(Mutex::new(ChannelsToSenders::new())),
        }
    }

    /// Runs an ERDOS node.
    ///
    /// The method never returns.
    pub fn run(&mut self) {
        debug!(self.config.logger, "Starting node {}", self.id);
        // Build a runtime with n threads.
        let runtime = Builder::new()
            .core_threads(self.config.num_worker_threads)
            .name_prefix(format!("node-{}", self.id))
            .build()
            .unwrap();
        runtime.block_on(self.async_run());
        runtime.shutdown_on_idle();
    }

    /// Runs an ERDOS node in a seperate OS thread.
    ///
    /// The method immediately returns.
    pub fn run_async(mut self) {
        // Copy dataflow graph to the other thread
        let graph = default_graph::clone();
        thread::spawn(move || {
            default_graph::set(graph);
            self.run();
        });
    }

    /// Splits a vector of TCPStreams into `ERDOSSender`s and `ERDOSReceiver`s.
    async fn split_data_streams(
        &mut self,
        mut streams: Vec<(NodeId, TcpStream)>,
    ) -> (Vec<ERDOSSender>, Vec<ERDOSReceiver>) {
        let mut sink_halves = Vec::new();
        let mut stream_halves = Vec::new();
        while let Some((node_id, stream)) = streams.pop() {
            // Use the message codec to divide the TCP stream data into messages.
            let framed = Framed::new(stream, MessageCodec::new());
            let (split_sink, split_stream) = framed.split();
            // Create an ERDOS receiver for the stream half.
            stream_halves.push(
                ERDOSReceiver::new(node_id, split_stream, self.channels_to_receivers.clone()).await,
            );

            // Create an ERDOS sender for the sink half.
            sink_halves.push(
                ERDOSSender::new(node_id, split_sink, self.channels_to_senders.clone()).await,
            );
        }
        (sink_halves, stream_halves)
    }

    /// Splits a vector of TCPStreams into `ControlMessageHandler`, `ControlSender`s and `ControlReceiver`s.
    async fn split_control_streams(
        &self,
        streams: Vec<(NodeId, TcpStream)>,
    ) -> (
        ControlMessageHandler,
        Vec<ControlSender>,
        Vec<ControlReceiver>,
    ) {
        let mut control_receivers = Vec::new();
        let mut control_senders = Vec::new();
        let (handler_tx, handler_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut channels_to_senders = HashMap::new();

        for (node_id, stream) in streams {
            // Use the message codec to divide the TCP stream data into messages.
            let framed = Framed::new(stream, ControlMessageCodec::new());
            let (split_sink, split_stream) = framed.split();
            // Create an control receiver for the stream half.
            control_receivers.push(ControlReceiver::new(
                node_id,
                split_stream,
                handler_tx.clone(),
            ));
            // Create an control sender for the sink half.
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            control_senders.push(ControlSender::new(node_id, split_sink, rx));
            channels_to_senders.insert(node_id, tx);
        }

        let control_handler = ControlMessageHandler::new(channels_to_senders, handler_rx);

        (control_handler, control_senders, control_receivers)
    }

    async fn run_operators(
        &mut self,
        mut control_handler: ControlMessageHandler,
    ) -> Result<(), String> {
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
        let num_local_operators = local_operators.len();

        let (operator_tx, rx_from_operators) = std::sync::mpsc::channel();
        let mut channels_to_operators = HashMap::new();

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

        // Wait for all operators to finish setting up
        let mut initialized_operators = HashSet::new();
        while initialized_operators.len() < num_local_operators {
            if let Ok(ControlMessage::OperatorInitialized(op_id)) = rx_from_operators.recv() {
                initialized_operators.insert(op_id);
            }
        }
        // Broadcast all operators initialized on current node
        eprintln!("{}: broadcasting that all ops are ready", self.id);
        control_handler
            .broadcast(ControlMessage::AllOperatorsInitializedOnNode(self.id))
            .map_err(|e| format!("Error broadcasting control message: {:?}", e))?;
        // Wait for all other nodes to finish setting up
        let num_nodes = self.config.data_addresses.len();
        let mut initialized_nodes = HashSet::new();
        initialized_nodes.insert(self.id);
        while initialized_nodes.len() < num_nodes {
            match control_handler.read().await {
                Ok(ControlMessage::AllOperatorsInitializedOnNode(node_id)) => {
                    initialized_nodes.insert(node_id);
                }
                Err(e) => {
                    return Err(format!("Error waiting for other nodes to set up: {:?}", e));
                }
                _ => (),
            }
        }
        eprintln!("All nodes are ready");
        // Tell all operators to run
        for (op_id, tx) in channels_to_operators {
            tx.send(ControlMessage::RunOperator(op_id))
                .map_err(|e| format!("Error telling operator to run: {}", e))?;
        }
        // Setup driver on the current node.
        if let Some(driver) = graph.get_driver(self.id) {
            for setup_hook in driver.setup_hooks {
                (setup_hook)(Arc::clone(&channel_manager));
            }
        }

        Ok(())
    }

    async fn async_run(&mut self) {
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
        let (control_handler, control_senders, control_receivers) = self.split_control_streams(control_streams).await;
        let (senders, receivers) = self.split_data_streams(data_streams).await;
        // Execute threads that send data to other nodes.
        let control_senders_fut = senders::run_control_senders(control_senders);
        let senders_fut = senders::run_senders(senders);
        // Execute threads that receive data from other nodes.
        let control_recvs_fut = receivers::run_control_receivers(control_receivers);
        let recvs_fut = receivers::run_receivers(receivers);
        // Execute operators.
        let ops_fut = self.run_operators(control_handler);
        // These threads only complete when a failure happens.
        let (senders_res, receivers_res, control_senders_res, control_receiver_res, ops_res) =
            future::join5(
                senders_fut,
                recvs_fut,
                control_senders_fut,
                control_recvs_fut,
                ops_fut,
            )
            .await;
        // TODO(ionel): Remove code after operators execute on tokio.
        if let (&Ok(_), &Ok(_)) = (&senders_res, &receivers_res) {
            // Single node, so block indefinitely
            loop {
                thread::sleep(Duration::from_secs(std::u64::MAX));
            }
        }
        if let Err(err) = senders_res {
            error!(self.config.logger, "Error with ERDOS senders: {:?}", err);
        }
        if let Err(err) = receivers_res {
            error!(self.config.logger, "Error with ERDOS receivers: {:?}", err);
        }
        if let Err(err) = control_senders_res {
            error!(self.config.logger, "Error with control senders: {:?}", err);
        }
        if let Err(err) = control_receiver_res {
            error!(
                self.config.logger,
                "Error with control receivers: {:?}", err
            );
        }
        if let Err(err) = ops_res {
            error!(
                self.config.logger,
                "Error running operators on node {:?}: {:?}", self.id, err
            );
        }
    }
}
