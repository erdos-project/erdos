use std::net::SocketAddr;

use crate::node::NodeId;

/// Stores the configuration parameters of a node.
#[derive(Clone)]
pub struct Configuration {
    /// The index of the node.
    pub index: NodeId,
    /// The number of worker threads the node has.
    pub num_worker_threads: usize,
    /// Mapping between node indices and socket addresses.
    pub addr_nodes: Vec<SocketAddr>,
    /// System-level logger.
    pub logger: slog::Logger,
}

impl Configuration {
    /// Creates a new node configuration.
    pub fn new(node_index: NodeId, addresses: Vec<SocketAddr>, num_worker_threads: usize) -> Self {
        Self {
            index: node_index,
            num_worker_threads,
            addr_nodes: addresses,
            logger: crate::get_terminal_logger(),
        }
    }

    /// Creates a node configuration from command line arguments.
    pub fn from_args(args: &clap::ArgMatches) -> Self {
        let num_threads = args
            .value_of("threads")
            .unwrap()
            .parse()
            .expect("Unable to parse number of worker threads");

        let addrs = args.value_of("addresses").unwrap();
        let mut addresses: Vec<SocketAddr> = Vec::new();
        for addr in addrs.split(",") {
            addresses.push(addr.parse().expect("Unable to parse socket address"));
        }
        let node_index = args
            .value_of("index")
            .unwrap()
            .parse()
            .expect("Unable to parse node index");
        assert!(
            node_index < addresses.len(),
            "Node index is larger than number of available nodes"
        );
        Self {
            index: node_index,
            num_worker_threads: num_threads,
            addr_nodes: addresses,
            logger: crate::get_terminal_logger(),
        }
    }

    pub fn node_address(&self) -> SocketAddr {
        self.addr_nodes[self.index].clone()
    }
}
