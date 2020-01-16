use futures::future;
use std::{sync::Arc, thread, time::Duration};
use tokio::{codec::Framed, net::TcpStream, prelude::*, runtime::Builder, sync::Mutex};

use crate::communication;
use crate::communication::{
    receivers, receivers::ERDOSReceiver, senders, senders::ERDOSSender, MessageCodec,
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
    async fn split_streams(
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
                ERDOSReceiver::new(node_id, split_stream, self.channels_to_receivers.clone())
                    .await,
            );

            // Create an ERDOS sender for the sink half.
            sink_halves.push(
                ERDOSSender::new(node_id, split_sink, self.channels_to_senders.clone()).await,
            );
        }
        (sink_halves, stream_halves)
    }

    async fn run_operators(&mut self) -> Result<(), String> {
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
        for operator_info in graph
            .get_operators()
            .into_iter()
            .filter(|op| op.node_id == self.id)
        {
            debug!(
                self.config.logger,
                "Executing operator {} on node {}", operator_info.id, operator_info.node_id
            );
            let channel_manager_copy = Arc::clone(&channel_manager);
            // Launch the operator as a separate async task.
            tokio::spawn(async move {
                (operator_info.runner)(channel_manager_copy).execute();
            });
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
        let streams = communication::create_tcp_streams(
            self.config.addr_nodes.clone(),
            self.id,
            &self.config.logger,
        )
        .await;
        let (senders, receivers) = self.split_streams(streams).await;
        // Execute threads that send data to other nodes.
        let senders_fut = senders::run_senders(senders);
        // Execute threads that receive data from other nodes.
        let recvs_fut = receivers::run_receivers(receivers);
        // Execute operators.
        let ops_fut = self.run_operators();
        // These threads only complete when a failure happens.
        let (senders_res, receivers_res, ops_res) =
            future::join3(senders_fut, recvs_fut, ops_fut).await;
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
        if let Err(err) = ops_res {
            error!(
                self.config.logger,
                "Error running operators on node {:?}: {:?}", self.id, err
            );
        }
    }
}
