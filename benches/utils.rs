use std::time::Duration;

use nix::{
    sys::{
        signal::{self, Signal},
        wait::waitpid,
    },
    unistd::{self, ForkResult, Pid},
};

use erdos::{
    dataflow::stream::{ExtractStream, IngestStream},
    node::{Node, NodeHandle},
    Configuration,
};

#[derive(Clone, Copy)]
pub enum BenchType {
    InterProcess,
    InterThread,
}

pub struct DataflowHandle {
    pub ingest_stream: IngestStream<usize>,
    pub extract_streams: Vec<ExtractStream<Vec<Duration>>>,
    num_nodes: usize,
    node_handle: Option<NodeHandle>,
    child_pids: Vec<Pid>,
}

impl DataflowHandle {
    pub fn new(
        num_nodes: usize,
        ingest_stream: IngestStream<usize>,
        extract_streams: Vec<ExtractStream<Vec<Duration>>>,
    ) -> Self {
        Self {
            num_nodes,
            ingest_stream,
            extract_streams,
            node_handle: None,
            child_pids: Vec::new(),
        }
    }

    /// Runs the dataflow asynchronously
    pub fn run(&mut self) {
        match self.num_nodes {
            0 => {
                let config = make_default_config();
                let node = Node::new(config);
                self.node_handle = Some(node.run_async())
            }
            num_nodes => {
                let data_addresses = (0..num_nodes)
                    .map(|x| {
                        format!("127.0.0.1:{}", 2 * x + 9000)
                            .parse()
                            .expect("Unable to parse socket address")
                    })
                    .collect();
                let control_addresses = (0..num_nodes)
                    .map(|x| {
                        format!("127.0.0.1:{}", 2 * x + 9001)
                            .parse()
                            .expect("Unable to parse socket address")
                    })
                    .collect();
                for i in 1..num_nodes {
                    match unistd::fork().unwrap() {
                        ForkResult::Parent { child } => self.child_pids.push(child),
                        ForkResult::Child => {
                            let config =
                                Configuration::new(i, data_addresses, control_addresses, 4, None);
                            let mut node = Node::new(config);
                            node.run();
                            std::process::exit(0);
                        }
                    }
                }
                let driver_config =
                    Configuration::new(0, data_addresses, control_addresses, 4, None);
                let node = Node::new(driver_config);
                self.node_handle = Some(node.run_async());
            }
        }
    }

    pub fn shutdown(&mut self) {
        for pid in self.child_pids.iter() {
            signal::kill(*pid, Signal::SIGTERM).unwrap();
            waitpid(*pid, None).unwrap();
        }
    }
}

pub fn make_default_config() -> Configuration {
    let data_addresses = vec![format!("127.0.0.1:{}", 9000)
        .parse()
        .expect("Unable to parse socket address")];
    let control_addresses = vec![format!("127.0.0.1:{}", 9001)
        .parse()
        .expect("Unable to parse socket address")];
    Configuration::new(0, data_addresses, control_addresses, 4, None)
}
