use std::{error::Error, fmt, net::SocketAddr};

use tokio::{
    runtime::Builder,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::{
    communication::{
        control_plane::notifications::{DriverNotification, QueryResponseType, QueryType},
        errors::CommunicationError,
    },
    dataflow::{
        graph::{GraphCompilationError, JobGraphId},
        Graph,
    },
    node::{Leader, Resources, WorkerNode},
    Configuration,
};

use super::{ExecutionState, WorkerId};

/// The error raised by the handles when executing commands from the drivers.
#[derive(Debug)]
pub enum HandleError {
    GraphCompilationError(GraphCompilationError),
    CommunicationError(String),
}

impl Error for HandleError {}

impl fmt::Display for HandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandleError::GraphCompilationError(err) => write!(f, "{}", err),
            HandleError::CommunicationError(err) => write!(f, "{}", err),
        }
    }
}

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
    pub fn new(leader_address: SocketAddr, logging_level: Option<tracing::Level>) -> Self {
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

        // Initialize a channel between the Handle and the Leader.
        // This channel is used by the Handle to submit requests to the Leader.
        let (leader_tx, leader_rx) = mpsc::channel(100);

        // Initialize a Leader.
        let mut leader_node = Leader::new(leader_address, leader_rx);
        let leader_task = tokio::spawn(async move { leader_node.run().await });
        Self {
            leader_handle: leader_tx,
            handle_id: 0,
            leader_task,
            logger_guard,
        }
    }

    pub async fn shutdown(&self) -> Result<(), HandleError> {
        // Send a shutdown message to the Leader.
        self.leader_handle
            .send(DriverNotification::Shutdown)
            .await
            .map_err(|_| {
                HandleError::CommunicationError(String::from(
                    "Error submitting Shutdown message to Leader.",
                ))
            })?;
        Ok(())
    }
}

/// A [`WorkerHandle`] is used by driver applications to submit ERDOS applications
/// to the ERDOS Leader, and query their execution progres.
pub struct WorkerHandle {
    /// A channel to communicate notifications to the underlying Worker.
    channel_to_worker: UnboundedSender<DriverNotification>,
    /// A channel to receive notifications from the underlying Worker.
    channel_from_worker: UnboundedReceiver<DriverNotification>,
    /// An ID for the WorkerHandle that mirrors the ID of the underlying Worker.
    handle_id: WorkerId,
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
            .thread_name(format!("Worker-{}", config.id))
            .enable_all()
            .build()
            .unwrap();

        // Initialize channels between the Handle and the Worker.
        // This channel is used by the Handle to submit requests to the Worker.
        let (channel_to_worker_tx, channel_to_worker_rx) = mpsc::unbounded_channel();
        // This channel is used by the Worker to respond to the Handle.
        let (channel_from_worker_tx, channel_from_worker_rx) = mpsc::unbounded_channel();

        // Initialize a Worker with the given index, and an empty set of Resources.
        // TODO (Sukrit): In the future, the index of the Worker should be generated
        // at runtime, and be of a type alias WorkerId for Uuid.
        let worker_resources = Resources::empty();
        let mut worker = WorkerNode::new(
            config.id,
            config.leader_address,
            config.data_plane_address,
            worker_resources,
            channel_to_worker_rx,
            channel_from_worker_tx,
            config.num_threads,
        );
        let worker_task = worker_runtime.spawn(async move { worker.run().await });
        Self {
            handle_id: config.id,
            channel_to_worker: channel_to_worker_tx,
            channel_from_worker: channel_from_worker_rx,
            worker_task,
            worker_runtime,
            logger_guard,
        }
    }

    // TODO (Sukrit): This function is kept different from the `submit` method
    // because all Workers need a copy of the JobGraph code, but only one of
    // them needs to submit it to the Leader. This should later be removed if
    // we choose to dynamically link the user applications into the Worker's
    // memory space.
    /// Registers the [`Graph`] for execution with the [`Worker`]s.
    pub fn register(&self, graph: Graph) -> Result<JobGraphId, HandleError> {
        // Compile the JobGraph and register it with the Worker.
        let job_graph = graph
            .compile()
            .map_err(HandleError::GraphCompilationError)?;
        let job_graph_id = job_graph.id();
        tracing::trace!(
            "WorkerHandle {} received a notification from the Driver \
                            to register JobGraph {} (ID={:?}).",
            self.handle_id,
            job_graph.name(),
            job_graph_id,
        );
        self.channel_to_worker
            .send(DriverNotification::RegisterGraph(job_graph))
            .map_err(|_| {
                HandleError::CommunicationError(String::from(
                    "Error registering the Graph with the Leader.",
                ))
            })?;

        Ok(job_graph_id)
    }

    /// Submits the [`Graph`] to the `Leader` for execution.
    ///
    /// This method automatically invokes the [`register`] method.
    pub fn submit(&self, graph: Graph) -> Result<JobGraphId, HandleError> {
        // Compile the JobGraph and register it with the Worker.
        let job_graph_id = self.register(graph)?;

        // Submit the JobGraph to the Leader.
        self.channel_to_worker
            .send(DriverNotification::SubmitGraph(job_graph_id.clone()))
            .map_err(|_| {
                HandleError::CommunicationError(String::from(
                    "Error submitting the Graph to the Leader.",
                ))
            })?;

        Ok(job_graph_id)
    }

    /// Retrieves the status of the `Graph` from the `Leader`.
    ///
    /// # Arguments
    /// - `graph_id`: The ID of the `Graph` whose status needs to be retrieved.
    fn job_graph_status(
        &mut self,
        graph_id: &JobGraphId,
    ) -> Result<ExecutionState, HandleError> {
        // Request the Worker for the status of the JobGraph.
        self.channel_to_worker
            .send(DriverNotification::Query(QueryType::JobGraphStatus(
                graph_id.clone(),
            )))
            .map_err(|_| {
                HandleError::CommunicationError(String::from(
                    "Error requesting the Ready status of the Graph from the Leader.",
                ))
            })?;

        // Wait for the response of the Query from the Worker.
        match self.channel_from_worker.blocking_recv() {
            Some(notification) => match notification {
                DriverNotification::QueryResponse(query_response) => match query_response {
                    QueryResponseType::JobGraphStatus(response_graph_id, job_graph_state) => {
                        if response_graph_id == *graph_id {
                            match job_graph_state {
                                Ok(result) => Ok(result),
                                Err(error) => Err(HandleError::CommunicationError(error.0)),
                            }
                        } else {
                            Err(HandleError::CommunicationError(String::from(
                                "Incorrect JobGraph status retrieved from the Leader.",
                            )))
                        }
                    }
                },
                _ => Err(HandleError::CommunicationError(String::from(
                    "Error retrieving the Ready status of the Graph from the Leader.",
                ))),
            },
            None => Err(HandleError::CommunicationError(String::from(
                "Error retrieving the Ready status of the Graph from the Leader.",
            ))),
        }
    }

    /// Checks if the [`Graph`] submitted to the [`Worker`] is ready for execution or executing.
    ///
    /// # Arguments
    /// - `graph_id`: The ID of the `Graph` returned by the [`submit`] or [`register`] methods.
    pub fn job_graph_ready(&mut self, graph_id: &JobGraphId) -> Result<bool, HandleError> {
        let job_graph_state = self.job_graph_status(graph_id)?;
        Ok(
            job_graph_state == ExecutionState::Ready
                || job_graph_state == ExecutionState::Executing,
        )
    }

    /// Retrieve the ID of the Worker underlying this handle.
    pub fn id(&self) -> WorkerId {
        self.handle_id
    }
}
