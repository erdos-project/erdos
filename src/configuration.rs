use std::net::SocketAddr;

use crate::node::NodeId;

/// Stores the configuration parameters of a node.
#[derive(Clone)]
pub struct Configuration {
    /// The index of the node.
    pub index: NodeId,
    /// The number of worker threads the node has.
    pub num_worker_threads: usize,
    /// Mapping between node indices and data socket addresses.
    pub data_addresses: Vec<SocketAddr>,
    /// Mapping between node indices and control socket addresses.
    pub control_addresses: Vec<SocketAddr>,
    /// System-level logger.
    pub logger: slog::Logger,
}

impl Configuration {
    /// Creates a new node configuration.
    pub fn new(node_index: NodeId, data_addresses: Vec<SocketAddr>, control_addresses: Vec<SocketAddr>, num_worker_threads: usize) -> Self {
        Self {
            index: node_index,
            num_worker_threads,
            data_addresses,
            control_addresses,
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

        let data_addrs = args.value_of("data-addresses").unwrap();
        let mut data_addresses: Vec<SocketAddr> = Vec::new();
        for addr in data_addrs.split(",") {
            data_addresses.push(addr.parse().expect("Unable to parse socket address"));
        }
        let control_addrs = args.value_of("control-addresses").unwrap();
        let mut control_addresses: Vec<SocketAddr> = Vec::new();
        for addr in control_addrs.split(",") {
            control_addresses.push(addr.parse().expect("Unable to parse socket address"));
        }
        let node_index = args
            .value_of("index")
            .unwrap()
            .parse()
            .expect("Unable to parse node index");
        assert!(
            node_index < data_addresses.len(),
            "Node index is larger than number of available nodes"
        );
        Self {
            index: node_index,
            num_worker_threads: num_threads,
            data_addresses,
            control_addresses,
            logger: crate::get_terminal_logger(),
        }
    }
}
