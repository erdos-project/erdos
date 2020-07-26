use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    fmt,
    sync::Arc,
};

use futures::lock::Mutex;
use petgraph::{
    dot::{self, Dot},
    stable_graph::{EdgeIndex, NodeIndex, StableGraph},
    visit::{DfsPostOrder, Reversed},
    Direction,
};

use crate::{dataflow::Timestamp, node::operator_event::OperatorEvent};

/// `RunnableEvent` is a data structure that is used to represent an event that is ready to be
/// executed.
///
/// A `RunnableEvent` is essentially an index into the lattice, with additional metadata to
/// prioritize events that are ready to run.
#[derive(Clone)]
pub struct RunnableEvent {
    /// The `node_index` is the index of the runnable event in the lattice.
    node_index: NodeIndex<u32>,
    /// The `timestamp` is the timestamp of the event indexed by the id.
    timestamp: Option<Timestamp>,
}

impl RunnableEvent {
    /// Creates a new instance of `RunnableEvent`.
    pub fn new(node_index: NodeIndex<u32>) -> Self {
        RunnableEvent {
            node_index,
            timestamp: None,
        }
    }

    /// Add a timestamp to the event.
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

// Implement the `Display` and `Debug` traits so that we can visualize the event.
impl fmt::Display for RunnableEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RunnableEvent(index: {}, Timestamp: {:?}",
            self.node_index.index(),
            self.timestamp
        )
    }
}

impl fmt::Debug for RunnableEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RunnableEvent(index: {}, Timestamp: {:?}",
            self.node_index.index(),
            self.timestamp
        )
    }
}

// Implement the equality criteria for a RunnableEvent.
impl Eq for RunnableEvent {}

impl PartialEq for RunnableEvent {
    // Two events are equal iff they are the same i.e. same index into the lattice.
    fn eq(&self, other: &RunnableEvent) -> bool {
        match self.node_index.index().cmp(&other.node_index.index()) {
            Ordering::Equal => true,
            _ => false,
        }
    }
}

// Implement the Ordering for a RunnableEvent.
impl Ord for RunnableEvent {
    fn cmp(&self, other: &RunnableEvent) -> Ordering {
        match (self.timestamp.as_ref(), other.timestamp.as_ref()) {
            (Some(ts1), Some(ts2)) => match ts1.cmp(ts2) {
                Ordering::Less => Ordering::Greater,
                Ordering::Greater => Ordering::Less,
                Ordering::Equal => {
                    // Break ties with the order of insertion into the lattice.
                    self.node_index
                        .index()
                        .cmp(&other.node_index.index())
                        .reverse()
                }
            },
            _ => {
                // We don't have enough information about the timestamps.
                // Order based on the order of insertion into the lattice.
                self.node_index
                    .index()
                    .cmp(&other.node_index.index())
                    .reverse()
            }
        }
    }
}

impl PartialOrd for RunnableEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// `ExecutionLattice` is a data structure that maintains [`OperatorEvent`]s in a
/// [dependency graph](https://en.wikipedia.org/wiki/Dependency_graph) according the partial order
/// defined.
///
/// Events can be added to the lattice using the `add_events` function, and retrieved using the
/// `get_event` function. The lattice requires a notification of the completion of the event using
/// the `mark_as_compeleted` function in order to unblock dependent events, and make them runnable.
///
/// # Example
/// The below example shows how to insert events into the Lattice and retrieve runnable events from
/// the lattice.
/// ```ignore
/// use erdos::node::{operator_event::OperatorEvent, lattice::ExecutionLattice};
/// use erdos::dataflow::Timestamp;
/// use futures::executor::block_on;
///
/// async fn async_main() {
///     let mut lattice: ExecutionLattice = ExecutionLattice::new();
///
///     // Add two events of timestamp 1 and 2 to the lattice with empty callbacks.
///     events = vec![
///         OperatorEvent::new(Timestamp::new(vec![1]), true, 0, HashSet::new(), HashSet::new(), || ())
///         OperatorEvent::new(Timestamp::new(vec![2]), true, 0, HashSet::new(), HashSet::new(), || ())
///     ];
///     lattice.add_events(events).await;
///
///     // Retrieve the first event from the lattice.
///     let (event_1, event_id_1) = lattice.get_event().await.unwrap();
///
///     // If we try to retrieve another event, we get None since we haven't marked the
///     // completion of the event with timestamp 1.
///     assert_eq!(lattice.get_event().await.is_none(), true);
///
///     // Mark the first event as completed.
///     lattice.mark_as_completed(event_id_1).await;
///
///     // Now, get the second event from the lattice.
///     let (event_2, event_id_2) = lattice.get_event().await.unwrap();
/// }
///
/// fn main() {
///     block_on(async_main());
/// }
/// ```
pub struct ExecutionLattice {
    /// The `forest` is the directed acyclic graph that maintains the dependency graph of the
    /// events. The relation A -> B means that A *depends on* B.This dependency also indicates that
    /// B precedes A (B < A) in the ordering. An event can be executed if it has no outbound edges.
    forest: Arc<Mutex<StableGraph<Option<OperatorEvent>, ()>>>,
    /// The `roots` are the roots of the forest of graphs, have no dependencies and can be run by
    /// the event executors.
    roots: Arc<Mutex<Vec<RunnableEvent>>>,
    /// The `run_queue` is the queue that maintains the events to be executed next. Note that this
    /// is different from the `roots` because a root is only removed once its marked as complete.
    run_queue: Arc<Mutex<BinaryHeap<RunnableEvent>>>,
}

impl ExecutionLattice {
    /// Creates a new instance of `ExecutionLattice`.
    pub fn new() -> Self {
        ExecutionLattice {
            forest: Arc::new(Mutex::new(StableGraph::new())),
            roots: Arc::new(Mutex::new(Vec::new())),
            run_queue: Arc::new(Mutex::new(BinaryHeap::new())),
        }
    }

    /// Add a batch of events to the lattice.
    ///
    /// This function moves the passed events into the lattice, and inserts the appropriate edges to
    /// existing events in the graph based on the partial order defined in [`OperatorEvent`].
    pub async fn add_events(&self, events: Vec<OperatorEvent>) {
        // Take locks over everything.
        let mut forest = self.forest.lock().await;
        let mut roots = self.roots.lock().await;
        let mut run_queue = self.run_queue.lock().await;

        for event in events {
            // Visits each node in the graph once.
            let mut dfs = DfsPostOrder::empty(Reversed(&*forest));
            // Caches preceding events to avoid adding redundant dependencies.
            // For example, A -> C is redundant if A -> B -> C.
            let mut preceding_events: HashSet<NodeIndex<u32>> = HashSet::new();
            // The added event depends on these nodes.
            let mut outgoing_edges: Vec<NodeIndex<u32>> = Vec::new();
            // Other nodes depend on the added event.
            let mut incoming_edges: Vec<NodeIndex<u32>> = Vec::new();
            // These nodes are no longer roots after the added event is inserted into the graph.
            let mut demoted_roots: Vec<RunnableEvent> = Vec::new();
            // Iterate through the forest with a DFS from each root to figure out where to add dependency edges.
            'dfs_roots: for root in roots.iter() {
                // Begin a DFS at the specified root.
                dfs.move_to(root.node_index);
                while let Some(nx) = dfs.next(Reversed(&*forest)) {
                    // If any of the current node's children precede the added event, then the current node also precedes
                    // the added event. By induction, the root would also precede the added event, so we can move on to the
                    // next root.
                    for child in forest.neighbors_directed(nx, Direction::Incoming) {
                        if preceding_events.contains(&child) {
                            preceding_events.insert(nx);
                            continue 'dfs_roots;
                        }
                    }

                    if let Some(node) = forest.node_weight(nx).unwrap().as_ref() {
                        if &event >= node {
                            // Check if any of the children of the current node depend on the added event.
                            // If children depend on the current DFS node and event > node, transfer
                            // dependencies from the current node to the added event.
                            let mut possible_transfer_dependencies: Vec<NodeIndex<u32>> =
                                Vec::new();
                            for child in forest.neighbors_directed(nx, Direction::Incoming) {
                                let child_node: &OperatorEvent =
                                    forest.node_weight(child).unwrap().as_ref().unwrap();
                                if child_node > &event {
                                    // The child depends on the added event.
                                    // Break the dependency from the DFS node to its child, and add a
                                    // dependency from the node to be added to the child.
                                    incoming_edges.push(child);
                                    possible_transfer_dependencies.push(child);
                                }
                            }

                            // If this event depends on the current node, then add an edge from this
                            // event to the current node.
                            if &event > node {
                                outgoing_edges.push(nx);
                                for child in possible_transfer_dependencies {
                                    let edge = forest.find_edge(child, nx).unwrap();
                                    forest.remove_edge(edge);
                                }
                                preceding_events.insert(nx);
                            }
                        } else {
                            // The currrent DFS node depends on the added event. Usually, this is resolved
                            // at a higher DFS level, but add edges if the node is root.
                            if forest.neighbors_directed(nx, Direction::Outgoing).count() == 0 {
                                incoming_edges.push(nx);
                                for n in run_queue.iter() {
                                    if n.node_index.index() == nx.index() {
                                        demoted_roots.push(n.clone());
                                    }
                                }
                            }
                        }
                    } else {
                        // Reached a node that is already executing, but hasn't been completed.
                        // The current node will probably get added as a root. Add dependencies
                        // between children and current event.
                        for child in forest.neighbors_directed(nx, Direction::Incoming) {
                            let child_node = forest.node_weight(child).unwrap().as_ref().unwrap();
                            if child_node > &event {
                                incoming_edges.push(child);
                            }
                        }
                    }
                }
            }

            // Add the node into the forest.
            let event_timestamp: Timestamp = event.timestamp.clone();
            let event_ix: NodeIndex<u32> = forest.add_node(Some(event));

            // Add edges indicating dependencies.
            for parent in outgoing_edges {
                forest.add_edge(event_ix, parent, ());
            }
            for child in incoming_edges {
                forest.add_edge(child, event_ix, ());
            }

            // Clean up the roots and the run queue, if any.
            // TODO (Sukrit) :: BinaryHeap does not provide a way to remove an element that is not at
            // the top of the heap. So, this particularly costly implementation clones the elements out
            // of the earlier run_queue, clears the run_queue and initializes it afresh with the set
            // difference of the old run_queue and the nodes to remove.
            // Since the invocation of this code is hopefully rare, we can optimize it later.
            if demoted_roots.len() > 0 {
                roots.retain(|e| !demoted_roots.contains(e));
                // The run queue needs to be reconstructed.
                let old_run_queue: Vec<RunnableEvent> = run_queue.drain().collect();
                for item in old_run_queue {
                    if !demoted_roots.contains(&item) {
                        run_queue.push(item);
                    }
                }
            }

            // If the added event depends on no others, then we can safely create a new root in the forest and
            // add the event to the run queue.
            if preceding_events.is_empty() {
                roots.push(RunnableEvent::new(event_ix).with_timestamp(event_timestamp.clone()));
                run_queue.push(RunnableEvent::new(event_ix).with_timestamp(event_timestamp));
            }
        }

        if forest.node_count() > 25 {
            slog::warn!(
                crate::get_terminal_logger(),
                "{} operator events queued in lattice. Increase number of operator executors or decrease incoming \
                message frequency to reduce load.", forest.node_count()
            )
        }
    }

    /// Retrieve an event to be executed from the lattice.
    ///
    /// This function retrieves an event that is not being executed by any other executor, along
    /// with a unique identifier for the event. This unique identifier needs to be passed to the
    /// [`ExecutionLattice::mark_as_completed`] function to remove the event from the lattice, and
    /// ensure that its dependencies are runnable.
    pub async fn get_event(&self) -> Option<(OperatorEvent, usize)> {
        // Take locks over everything.
        let mut forest = self.forest.lock().await;
        let _roots = self.roots.lock().await;
        let mut run_queue = self.run_queue.lock().await;

        // Retrieve the event
        match run_queue.pop() {
            Some(runnable_event) => {
                let event = forest[runnable_event.node_index].take();
                Some((event.unwrap(), runnable_event.node_index.index()))
            }
            None => None,
        }
    }

    /// Mark an event as completed, and break the dependency from this event to its children.
    ///
    /// `event_id` is the unique identifer returned by the [`ExecutionLattice::get_event`]
    /// invocation.
    pub async fn mark_as_completed(&self, event_id: usize) {
        // Take locks over everything.
        let mut forest = self.forest.lock().await;
        let mut roots = self.roots.lock().await;
        let mut run_queue = self.run_queue.lock().await;

        let node_idx: NodeIndex<u32> = NodeIndex::new(event_id);

        // Throw an error if the item was not in the roots.
        let event = RunnableEvent::new(node_idx);
        let event_idx = roots
            .iter()
            .position(|e| e == &event)
            .expect("Item must be in the roots of the lattice.");
        roots.remove(event_idx);

        // Go over the children of the node, and check which ones have no dependencies on other
        // nodes, and add them to the list of the roots.
        let mut edges_to_remove: Vec<NodeIndex<u32>> = Vec::new();
        for child_id in forest.neighbors_directed(node_idx, Direction::Incoming) {
            // Promote child to root if it does not depend on any other events.
            if forest
                .neighbors_directed(child_id, Direction::Outgoing)
                .count()
                == 1
            {
                let timestamp: Timestamp = forest[child_id].as_ref().unwrap().timestamp.clone();
                let child = RunnableEvent::new(child_id).with_timestamp(timestamp);
                roots.push(child.clone());
                run_queue.push(child);
            }

            // Remove the edge from the child.
            edges_to_remove.push(child_id);
        }

        // Remove the edges from the forest.
        for child_id in edges_to_remove {
            let edge_id: EdgeIndex<u32> = forest.find_edge(child_id, node_idx).unwrap();
            forest.remove_edge(edge_id);
        }

        // Remove the completed event from the forest.
        forest.remove_node(node_idx);
    }

    /// Convert graph to string in DOT format.
    #[allow(dead_code)]
    pub async fn to_dot(&self) -> String {
        // Lock the graph.
        let forest = self.forest.lock().await;
        let roots = self.roots.lock().await;
        let graph = forest.map(
            |nx, n| {
                n.as_ref().map_or_else(
                    || {
                        roots
                            .iter()
                            .filter(|r| r.node_index == nx)
                            .next()
                            .map_or_else(|| "Executing".to_string(), |r| format!("Executing {}", r))
                    },
                    |x| x.to_string(),
                )
            },
            |_, e| *e,
        );
        format!(
            "{:?}",
            Dot::with_config(&graph, &[dot::Config::EdgeNoLabel])
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataflow::Timestamp;
    use futures::executor::block_on;

    /// Test that a root gets added correctly to an empty lattice and that we can retrieve it from
    /// the lattice.
    #[test]
    fn test_root_addition() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![OperatorEvent::new(
            Timestamp::new(vec![1]),
            false,
            0,
            HashSet::new(),
            HashSet::new(),
            || (),
        )];
        block_on(lattice.add_events(events));

        // Ensure that the correct event is returned by the lattice.
        let (event, _event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0], 1,
            "The wrong event was returned by the lattice."
        );

        // Ensure that only one event is returned by the lattice.
        let next_event = block_on(lattice.get_event());
        assert!(next_event.is_none(), "Expected no event from the lattice.");
    }

    /// Test that the addition of two messages of the same timestamp leads to no dependencies.
    #[test]
    fn test_concurrent_messages() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
        ];
        block_on(lattice.add_events(events));

        // Check the first event is returned correctly by the lattice.
        let (event, _event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0], 1,
            "The wrong event was returned by the lattice."
        );

        // Check that the other event is returned without marking the first one as completed.
        // This shows that they can be executed concurrently.
        let (event_2, _event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp.time[0], 1,
            "The wrong event was returned by the lattice."
        );
    }

    /// Test that the addition of two messages of same timestamp, with their watermark ensures that
    /// the watermark runs after both of the messages are marked as finished executing.
    #[test]
    fn test_watermark_post_concurrent_messages() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
        ];
        block_on(lattice.add_events(events));
        // Check that the first event is returned correctly by the lattice.
        let (event, event_id) = block_on(lattice.get_event()).unwrap();
        assert!(
            event.timestamp.time[0] == 1 && !event.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );

        // Check that the first event is returned correctly by the lattice.
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_2.timestamp.time[0] == 1 && !event.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        let no_event = block_on(lattice.get_event());
        assert!(no_event.is_none(), "Expected no event from the lattice.");

        // Mark one of the event as completed, and still don't expect an event.
        block_on(lattice.mark_as_completed(event_id));

        let no_event_2 = block_on(lattice.get_event());
        assert!(no_event_2.is_none(), "Expected no event from the lattice.");

        // Mark the other as completed and expect a Watermark.
        block_on(lattice.mark_as_completed(event_id_2));

        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_3.timestamp.time[0] == 1 && event_3.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
    }

    /// Test that the addition of three watermark messages in reverse order, leads to them being
    /// executed in the correct order.
    #[test]
    fn test_unordered_watermark() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![
            OperatorEvent::new(
                Timestamp::new(vec![3]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![2]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
        ];
        block_on(lattice.add_events(events));

        let (event, event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0], 1,
            "The wrong event was returned by the lattice."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id));
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp.time[0], 2,
            "The wrong event was returned by the lattice."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_2));
        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_3.timestamp.time[0], 3,
            "The wrong event was returned by the lattice."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
    }

    /// Test that the addition of messages of different timestamps leads to concurrent execution.
    #[test]
    fn test_concurrent_messages_diff_timestamps() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![
            OperatorEvent::new(
                Timestamp::new(vec![3]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![2]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
        ];
        block_on(lattice.add_events(events));

        let (event, _event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0], 1,
            "The wrong event was returned by the lattice."
        );
        let (event_2, _event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp.time[0], 2,
            "The wrong event was returned by the lattice."
        );
        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_3.timestamp.time[0], 3,
            "The wrong event was returned by the lattice."
        );
    }

    /// Test that concurrent messages are followed by their watermarks.
    #[test]
    fn test_concurrent_messages_watermarks_diff_timestamps() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![
            OperatorEvent::new(
                Timestamp::new(vec![3]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![2]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![2]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![3]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
        ];
        block_on(lattice.add_events(events));
        let (event, event_id) = block_on(lattice.get_event()).unwrap();
        assert!(
            event.timestamp.time[0] == 1 && !event.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_2.timestamp.time[0] == 2 && !event_2.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        let (event_3, event_id_3) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_3.timestamp.time[0] == 3 && !event_3.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id));
        let (event_4, event_id_4) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_4.timestamp.time[0] == 1 && event_4.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_4));
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_2));
        let (event_5, event_id_5) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_5.timestamp.time[0] == 2 && event_5.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_3));
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_5));
        let (event_6, event_id_6) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_6.timestamp.time[0] == 3 && event_6.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_6));
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
    }

    /// Test that the messages are retrieved in the timestamp order.
    /// Added to address the bug where we would have starvation due to
    /// watermark messages of earlier timestamps being blocked by non-watermark
    /// callbacks of newer timestamps.
    #[test]
    fn test_ordered_concurrent_execution() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![
            OperatorEvent::new(
                Timestamp::new(vec![2]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![1]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::new(vec![3]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
        ];
        block_on(lattice.add_events(events));

        let (event, _event_id) = block_on(lattice.get_event()).unwrap();
        assert!(
            event.timestamp.time[0] == 1 && event.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );

        let (event_2, _event_id_2) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_2.timestamp.time[0] == 2 && !event_2.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_3.timestamp.time[0] == 3 && !event_3.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
    }
}
