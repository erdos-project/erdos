mod control_message_codec;
mod control_message_handler;
mod endpoints;
mod errors;
mod message_codec;
mod serializable;

pub mod pusher;
pub mod receivers;
pub mod senders;

// Re-export structs as if they were defined here.
pub use control_message_codec::ControlMessageCodec;
pub use control_message_handler::ControlMessageHandler;
pub(crate) use endpoints::{RecvEndpoint, SendEndpoint};
pub use errors::{CodecError, CommunicationError, TryRecvError};
pub use message_codec::MessageCodec;
pub use pusher::{Pusher, PusherT};
pub use serializable::Serializable;

use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::BytesMut;
use futures::future;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    prelude::*,
    time::delay_for,
};

use crate::{
    dataflow::{stream::StreamId, Data},
    node::node::NodeId,
    OperatorId,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    AllOperatorsInitializedOnNode(NodeId),
    OperatorInitialized(OperatorId),
    RunOperator(OperatorId),
    DataSenderInitialized(NodeId),
    DataReceiverInitialized(NodeId),
    ControlSenderInitialized(NodeId),
    ControlReceiverInitialized(NodeId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMetadata {
    pub stream_id: StreamId,
}

#[derive(Clone)]
pub enum InterProcessMessage {
    Serialized {
        metadata: MessageMetadata,
        bytes: BytesMut,
    },
    Deserialized {
        metadata: MessageMetadata,
        data: Arc<dyn Serializable + Send + Sync>,
    },
}

impl InterProcessMessage {
    pub fn new_serialized(bytes: BytesMut, metadata: MessageMetadata) -> Self {
        Self::Serialized { metadata, bytes }
    }

    pub fn new_deserialized(
        data: Arc<dyn Serializable + Send + Sync>,
        stream_id: StreamId,
    ) -> Self {
        Self::Deserialized {
            metadata: MessageMetadata { stream_id },
            data,
        }
    }
}

/// Returns a vec of TCPStreams; one for each node pair.
///
/// The function creates a TCPStream to each node address. The node address vector stores
/// the network address of each node, and is indexed by node id.
pub async fn create_tcp_streams(
    node_addrs: Vec<SocketAddr>,
    node_id: NodeId,
    logger: &slog::Logger,
) -> Vec<(NodeId, TcpStream)> {
    let node_addr = node_addrs[node_id].clone();
    // Connect to the nodes that have a lower id than the node.
    let connect_streams_fut = connect_to_nodes(node_addrs[..node_id].to_vec(), node_id, logger);
    // Wait for connections from the nodes that have a higher id than the node.
    let stream_fut = await_node_connections(node_addr, node_addrs.len() - node_id - 1, logger);
    // Wait until all connections are established.
    match future::try_join(connect_streams_fut, stream_fut).await {
        Ok((mut streams, await_streams)) => {
            // Streams contains a TCP stream for each other node.
            streams.extend(await_streams);
            streams
        }
        Err(e) => {
            slog::error!(
                logger,
                "Node {}: creating TCP streams errored with {:?}",
                node_id,
                e
            );
            panic!(
                "Node {}: creating TCP streams errored with {:?}",
                node_id, e
            )
        }
    }
}

/// Connects to all addresses and sends node id.
///
/// The function returns a vector of `(NodeId, TcpStream)` for each connection.
async fn connect_to_nodes(
    addrs: Vec<SocketAddr>,
    node_id: NodeId,
    logger: &slog::Logger,
) -> Result<Vec<(NodeId, TcpStream)>, std::io::Error> {
    let mut connect_futures = Vec::new();
    // For each node address, launch a task that tries to create a TCP stream to the node.
    for addr in addrs.iter() {
        connect_futures.push(connect_to_node(addr, node_id, logger));
    }
    // Wait for all tasks to complete successfully.
    let tcp_results = future::try_join_all(connect_futures).await?;
    let streams: Vec<(NodeId, TcpStream)> = (0..tcp_results.len()).zip(tcp_results).collect();
    Ok(streams)
}

/// Creates TCP stream connection to an address and writes the node id on the TCP stream.
///
/// The function keeps on retrying until it connects successfully.
async fn connect_to_node(
    dst_addr: &SocketAddr,
    node_id: NodeId,
    logger: &slog::Logger,
) -> Result<TcpStream, std::io::Error> {
    // Keeps on reatying to connect to `dst_addr` until it succeeds.
    let mut last_err_msg_time = Instant::now();
    loop {
        match TcpStream::connect(dst_addr).await {
            Ok(mut stream) => {
                stream.set_nodelay(true).expect("couldn't disable Nagle");
                // Send the node id so that the TCP server knows with which
                // node the connection was established.
                let mut buffer: Vec<u8> = Vec::new();
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buffer, node_id as u32)?;
                loop {
                    match stream.write(&buffer[..]).await {
                        Ok(_) => return Ok(stream),
                        Err(e) => {
                            error!(
                                logger,
                                "Node {}: could not send node id to {}; error {}; retrying in 100 ms",
                                node_id,
                                dst_addr,
                                e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                // Only print connection errors every 1s.
                let now = Instant::now();
                if now.duration_since(last_err_msg_time) >= Duration::from_secs(1) {
                    error!(
                        logger,
                        "Node {}: could not connect to {}; error {}; retrying",
                        node_id,
                        dst_addr,
                        e
                    );
                    last_err_msg_time = now;
                }
                // Wait a bit until it tries to connect again.
                delay_for(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Awaiting for connections from `expected_conns` other nodes.
///
/// Upon a new connection, the function reads from the stream the id of the node that initiated
/// the connection.
async fn await_node_connections(
    addr: SocketAddr,
    expected_conns: usize,
    logger: &slog::Logger,
) -> Result<Vec<(NodeId, TcpStream)>, std::io::Error> {
    let mut await_futures = Vec::new();
    let mut listener = TcpListener::bind(&addr).await?;
    // Awaiting for `expected_conns` conections.
    for _ in 0..expected_conns {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true).expect("couldn't disable Nagle");
        // Launch a task that reads the node id from the TCP stream.
        await_futures.push(read_node_id(stream, logger));
    }
    // Await until we've received `expected_conns` node ids.
    Ok(future::try_join_all(await_futures).await?)
}

/// Reads a node id from a TCP stream.
///
/// The method is used to discover the id of the node that initiated the connection.
async fn read_node_id(
    mut stream: TcpStream,
    logger: &slog::Logger,
) -> Result<(NodeId, TcpStream), std::io::Error> {
    let mut buffer = [0u8; 4];
    match stream.read_exact(&mut buffer).await {
        Ok(n) => n,
        Err(e) => {
            error!(logger, "failed to read from socket; err = {:?}", e);
            return Err(e);
        }
    };
    let node_id: u32 = NetworkEndian::read_u32(&buffer);
    Ok((node_id as NodeId, stream))
}
