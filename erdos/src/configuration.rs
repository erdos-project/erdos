use std::net::SocketAddr;

use tracing::Level;

use crate::node::NodeId;

/// Stores the configuration parameters of a [`node`](crate::node::Node).
#[derive(Clone)]
pub struct Configuration {
    /// The index of the node.
    pub index: NodeId,
    /// The number of OS threads the node will use.
    pub num_threads: usize,
    /// Mapping between node indices and control socket addresses.
    pub leader_address: SocketAddr,
    /// Mapping between node indices and data socket addresses.
    pub data_addresses: Vec<SocketAddr>,
    /// DOT file to export dataflow graph.
    pub graph_filename: Option<String>,
    /// The logging level of the logger initialized by ERDOS.
    /// If `None`, ERDOS will not initialize a logger.
    /// Defaults to [`Level::DEBUG`] when compiling in debug mode,
    /// [`Level::INFO`] when compiling in release mode.
    ///
    /// While [`tracing`] provides extensions for connecting additional
    /// subscribers, note that these may impact performance.
    pub logging_level: Option<Level>,
}

impl Configuration {
    /// Creates a new node configuration.
    pub fn new(
        node_index: NodeId,
        leader_address: SocketAddr,
        data_addresses: Vec<SocketAddr>,
        num_threads: usize,
    ) -> Self {
        let log_level = if cfg!(debug_assertions) {
            Some(Level::DEBUG)
        } else {
            Some(Level::INFO)
        };
        Self {
            index: node_index,
            num_threads,
            leader_address,
            data_addresses,
            graph_filename: None,
            logging_level: log_level,
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
        for addr in data_addrs.split(',') {
            data_addresses.push(addr.parse().expect("Unable to parse socket address"));
        }
        let leader_address: SocketAddr = args
            .value_of("address")
            .unwrap()
            .parse()
            .expect("Unable to parse the address of the Leader node.");
        let node_index = args
            .value_of("index")
            .unwrap()
            .parse()
            .expect("Unable to parse node index");
        // assert!(
        //     node_index < data_addresses.len(),
        //     "Node index is larger than number of available nodes"
        // );
        let graph_filename_arg = args.value_of("graph-filename").unwrap();
        let graph_filename = if graph_filename_arg.is_empty() {
            None
        } else {
            Some(graph_filename_arg.to_string())
        };
        let log_level = match args.occurrences_of("verbose") {
            0 => None,
            1 => Some(Level::WARN),
            2 => Some(Level::INFO),
            3 => Some(Level::DEBUG),
            _ => Some(Level::TRACE),
        };

        Self {
            index: node_index,
            num_threads,
            leader_address,
            data_addresses,
            graph_filename,
            logging_level: log_level,
        }
    }

    /// Upon executing, exports the dataflow graph as a
    /// [DOT file](https://en.wikipedia.org/wiki/DOT_(graph_description_language)).
    pub fn export_dataflow_graph(mut self, filename: &str) -> Self {
        self.graph_filename = Some(filename.to_string());
        self
    }

    /// Sets the logging level.
    pub fn with_logging_level(mut self, level: Level) -> Self {
        self.logging_level = Some(level);
        self
    }

    /// ERDOS will not initialize a logger if this method is called.
    pub fn disable_logger(mut self) -> Self {
        self.logging_level = None;
        self
    }
}
