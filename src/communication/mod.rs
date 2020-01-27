mod control_message_codec;
mod endpoints;
mod errors;
mod message_codec;
pub mod pusher;
pub mod receivers;
pub mod senders;
mod serializable;

// Re-export structs as if they were defined here.
pub use crate::communication::control_message_codec::ControlMessageCodec;
pub use crate::communication::endpoints::{RecvEndpoint, SendEndpoint};
pub use crate::communication::errors::{CodecError, CommunicationError, TryRecvError};
pub use crate::communication::message_codec::MessageCodec;
pub use crate::communication::pusher::{ControlPusher, Pusher, PusherT};
pub use crate::communication::serializable::Serializable;

use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::BytesMut;
use futures::future;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::thread::sleep;
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    prelude::*,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use crate::{dataflow::stream::StreamId, node::node::NodeId, OperatorId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    AllOperatorsInitialized,
    AllOperatorsInitializedOnNode(NodeId),
    OperatorInitialized(OperatorId),
    RunOperator(OperatorId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    pub stream_id: StreamId,
    pub data_size: usize,
}

#[derive(Debug, Clone)]
pub struct SerializedMessage {
    pub header: MessageHeader,
    pub data: BytesMut,
}

impl SerializedMessage {
    pub fn new(data: BytesMut, stream_id: StreamId) -> Self {
        Self {
            header: MessageHeader {
                stream_id,
                data_size: data.len(),
            },
            data,
        }
    }
}

pub struct ControlMessageHandler {
    tx: ControlPusher,
    rx: UnboundedReceiver<ControlMessage>,
}

impl ControlMessageHandler {
    pub fn new(
        channels_to_senders: HashMap<NodeId, UnboundedSender<ControlMessage>>,
        handler_rx: UnboundedReceiver<ControlMessage>,
    ) -> Self {
        Self {
            tx: ControlPusher::new(channels_to_senders),
            rx: handler_rx,
        }
    }

    pub fn send(&mut self, node_id: NodeId, msg: ControlMessage) -> Result<(), CommunicationError> {
        self.tx.send(node_id, msg)
    }

    pub fn broadcast(&mut self, msg: ControlMessage) -> Result<(), CommunicationError> {
        self.tx.broadcast(msg)
    }

    pub async fn read(&mut self) -> Result<ControlMessage, CommunicationError> {
        self.rx.recv().await.ok_or(CommunicationError::Disconnected)
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
    let (mut streams, await_streams) = future::try_join(connect_streams_fut, stream_fut)
        .await
        .unwrap();
    // Streams contains a TCP stream for each other node.
    streams.extend(await_streams);
    streams
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
    loop {
        match TcpStream::connect(dst_addr).await {
            Ok(mut stream) => {
                stream.set_nodelay(true).expect("couldn't disable Nagle");
                // Send the node id so that the TCP server knows with which
                // node the connection was established.
                let mut buffer: Vec<u8> = Vec::new();
                buffer.write_u32::<NetworkEndian>(node_id as u32)?;
                loop {
                    match stream.write(&buffer[..]).await {
                        Ok(_) => return Ok(stream),
                        Err(e) => {
                            error!(
                                logger,
                                "could not send node id to {}; error {}; retrying in 100 ms",
                                dst_addr,
                                e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    logger,
                    "could not connect to {}; error {}; retrying in 100 ms", dst_addr, e
                );
                // Wait a bit until it tries to connect again.
                sleep(Duration::from_millis(100));
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
