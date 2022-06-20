use std::net::SocketAddr;

use tokio::{runtime::Builder, sync::mpsc, task::JoinHandle};
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::{
    communication::{CommunicationError, DriverNotification},
    dataflow::graph::{default_graph, JobGraph},
    node::{LeaderNode, Resources, WorkerNode},
    Configuration, Uuid,
};

/// A [`LeaderHandle`] is used by driver applications to interact
/// with the Leader node running on their local instance.
pub struct LeaderHandle {
    /// A handle to communicate notifications to the underlying Leader.
    leader_handle: mpsc::Sender<DriverNotification>,
    /// An ID for the LeaderHandle that mirrors the ID of the underlying Leader.
    handle_id: usize,
    /// A handle for the asynchronously running Leader task.
    leader_task: JoinHandle<Result<(), CommunicationError>>,
    /// A handle for the Logging subsystem that flushes the logs when dropped.
    logger_guard: Option<WorkerGuard>,
}

impl LeaderHandle {
    pub(crate) fn new(leader_address: SocketAddr, logging_level: Option<tracing::Level>) -> Self {
        // Initialize the logger.
        let logger_guard = if let Some(logging_level) = logging_level {
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

        tracing::debug!("Initialized a LeaderHandle!");

        // Initialize a channel between the Handle and the Leader.
        // This channel is used by the Handle to submit requests to the Leader.
        let (leader_tx, leader_rx) = mpsc::channel(100);

        // Initialize a Leader.
        let mut leader_node = LeaderNode::new(leader_address, leader_rx);
        let leader_task = tokio::spawn(async move { leader_node.run().await });
        Self {
            leader_handle: leader_tx,
            handle_id: 0,
            leader_task,
            logger_guard,
        }
    }

    pub async fn shutdown(&self) -> Result<(), CommunicationError> {
        // Send a shutdown message to the Leader.
        self.leader_handle
            .send(DriverNotification::Shutdown)
            .await?;
        Ok(())
    }
}

/// A [`WorkerHandle`] is used by driver applications to submit ERDOS applications
/// to the ERDOS Leader, and query their execution progres.
pub struct WorkerHandle {
    /// A handle to communicate notifications to the underlying Worker.
    worker_handle: mpsc::Sender<DriverNotification>,
    /// An ID for the WorkerHandle that mirrors the ID of the underlying Worker.
    handle_id: usize,
    /// A handle for the asynchronously running Worker task.
    worker_task: JoinHandle<Result<(), CommunicationError>>,
    /// A handle to the Tokio runtime spawned for this Worker.
    worker_runtime: tokio::runtime::Runtime,
    /// A handle for the Logging subsystem that flushes the logs when dropped.
    logger_guard: Option<WorkerGuard>,
}

impl WorkerHandle {
    pub fn new(config: Configuration) -> Self {
        // Initialize the logger.
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

        // Build a Tokio runtime.
        let worker_runtime = Builder::new_multi_thread()
            .worker_threads(config.num_threads)
            .thread_name(format!("Worker-{}", config.index))
            .enable_all()
            .build()
            .unwrap();

        // Initialize a channel between the Handle and the Worker.
        // This channel is used by the Handle to submit requests to the Worker.
        let (worker_tx, worker_rx) = mpsc::channel(100);

        // Initialize a Worker with the given index, and an empty set of Resources.
        // TODO (Sukrit): In the future, the index of the Worker should be generated
        // at runtime, and be of a type alias WorkerId for Uuid.
        let worker_resources = Resources::empty();
        let worker_id = config.index;
        let mut worker_node = WorkerNode::new(
            worker_id,
            config.leader_address,
            worker_resources,
            worker_rx,
        );
        let worker_task = worker_runtime.spawn(async move { worker_node.run().await });
        Self {
            handle_id: worker_id,
            worker_handle: worker_tx,
            worker_task,
            worker_runtime,
            logger_guard,
        }
    }

    // TODO (Sukrit): Take as input a Graph handle that is built by the user.
    // We should expose the `AbstractGraph` structure as a `GraphBuilder` and
    // consume that in the `submit` method to prevent the users from making
    // any further changes to it.
    pub fn submit(&self) {
        // Get the graph abstraction and submit it to the Worker.
        let job_graph = (default_graph::clone()).compile();
        let _ = self
            .worker_handle
            .send(DriverNotification::SubmitGraph(job_graph));
    }
}
