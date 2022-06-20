use tokio::{sync::mpsc, task::JoinHandle};
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::{
    communication::{CommunicationError, DriverNotification},
    dataflow::graph::{JobGraph, default_graph},
    node::{Resources, WorkerNode},
    Configuration, Uuid,
};

/// A [`Client`] is used by driver applications to submit ERDOS applications
/// to the ERDOS Leader, and query their execution progres.
pub struct Client {
    /// A handle to communicate notifications to the underlying Worker.
    worker_handle: mpsc::Sender<DriverNotification>,
    /// An ID for the Client that mirrors the ID of the underlying Worker.
    client_id: usize,
    /// A handle for the asynchronously running Worker task.
    worker_task: JoinHandle<Result<(), CommunicationError>>,
}

impl Client {
    pub fn new(config: Configuration) -> Self {
        // Setup the logger.
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

        // Initialize a channel between the Client and the Worker.
        // This channel is used by the Client to submit requests to the Worker.
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
        let worker_task = tokio::spawn(async move { worker_node.run().await });
        Self {
            client_id: worker_id,
            worker_handle: worker_tx,
            worker_task,
        }
    }

    // TODO (Sukrit): Take as input a Graph handle that is built by the user.
    // We should expose the `AbstractGraph` structure as a `GraphBuilder` and
    // consume that in the `submit` method to prevent the users from making
    // any further changes to it.
    pub fn submit(&self) {
        // Get the graph abstraction and submit it to the Worker.
        let job_graph = (default_graph::clone()).compile();
        let _ = self.worker_handle.send(DriverNotification::SubmitGraph(job_graph));
    }
}
