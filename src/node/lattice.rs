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
/// [dependency graph](https://en.wikipedia.org/wiki/Dependency_graph) according to the partial 
/// order defined.
///
/// Events can be added to the lattice using the `add_events` function, and retrieved using the
/// `get_event` function. The lattice requires a notification of the completion of the event using
/// the `mark_as_completed` function in order to unblock dependent events, and make them runnable.
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
///         OperatorEvent::new(Timestamp::Time(vec![1]), 
///             true, 0, HashSet::new(), HashSet::new(), || ())
///         OperatorEvent::new(Timestamp::Time(vec![2]), 
///             true, 0, HashSet::new(), HashSet::new(), || ())
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
    /// The `leaves` are the leaves of the forest of graphs, have no dependencies and can be run by
    /// the event executors.
    leaves: Arc<Mutex<Vec<RunnableEvent>>>,
    /// The `run_queue` is the queue that maintains the events to be executed next. Note that this
    /// is different from the `leaves` because a leaf is only removed once its marked as complete.
    run_queue: Arc<Mutex<BinaryHeap<RunnableEvent>>>,
}

impl ExecutionLattice {
    /// Creates a new instance of `ExecutionLattice`.
    pub fn new() -> Self {
        ExecutionLattice {
            forest: Arc::new(Mutex::new(StableGraph::new())),
            leaves: Arc::new(Mutex::new(Vec::new())),
            run_queue: Arc::new(Mutex::new(BinaryHeap::new())),
        }
    }

    /// Add a batch of events to the lattice.
    ///
    /// This function moves the passed events into the lattice, and inserts the appropriate edges 
    /// to existing events in the graph based on the partial order defined in [`OperatorEvent`].
    pub async fn add_events(&self, events: Vec<OperatorEvent>) {
        // Take locks over everything.
        let mut forest = self.forest.lock().await;
        let mut leaves = self.leaves.lock().await;
        let mut run_queue = self.run_queue.lock().await;

        // If add_events becomes a bottleneck, look into changing the insertion algorithm to 
        // perform only 1 DFS instead of 1 per event. This could lead to more complex code to deal 
        // with dependency interactions among the batch of inserted events.
        for added_event in events {
            // Begins a DFS from each leaf, traversing the graph in the opposite direction of the 
            // edges.  The purpose of this search is to find where new edges representing 
            // dependencies must be added for the new event.
            // If the DFS becomes a performance bottleneck, consider searching from the roots of 
            // the forest as an optimization under the assumption that newly inserted events will 
            // likely depend on blocked already-inserted events.
            let mut dfs_from_leaf = DfsPostOrder::empty(Reversed(&*forest));
            // Caches preceding events to avoid adding redundant dependencies.
            // For example, A -> C is redundant if A -> B -> C.
            let mut preceding_events: HashSet<NodeIndex<u32>> = HashSet::new();
            // The added event depends on these nodes.
            let mut children: HashSet<NodeIndex<u32>> = HashSet::new();
            // Other nodes depend on the added event.
            let mut parents: HashSet<NodeIndex<u32>> = HashSet::new();
            // These nodes are no longer leaves after the added event is inserted into the graph.
            let mut demoted_leaves: Vec<NodeIndex> = Vec::new();
            // These edges are redundant and must be removed.
            let mut redundant_edges: Vec<EdgeIndex> = Vec::new();
            // Iterate through the forest with a DFS from each leaf to figure out where to add 
            // dependency edges.
            'dfs_leaves: for leaf in leaves.iter() {
                // Begin a reverse postorder DFS from the specified leaf.
                dfs_from_leaf.move_to(leaf.node_index);
                while let Some(visited_node_idx) = dfs_from_leaf.next(Reversed(&*forest)) {
                    // If any of the current node's parents already precede the added event, then 
                    // the current node also precedes the added event. Due to the reverse 
                    // post-order DFS from the leaves, the added event must already depend on an 
                    // ancenstor of the current node so we can skip this node because the 
                    // dependency is already established.
                    for parent in forest.neighbors_directed(visited_node_idx, Direction::Incoming) {
                        if preceding_events.contains(&parent) {
                            preceding_events.insert(visited_node_idx);
                            continue 'dfs_leaves;
                        }
                    }

                    if let Some(visited_event) =
                        forest.node_weight(visited_node_idx).unwrap().as_ref()
                    {
                        match added_event.cmp(visited_event) {
                            Ordering::Less => {
                                // The visited event depends on the added event.
                                // Add a dependency to the added event if the current node is a 
                                // leaf.  Otherwise, the dependency is resolved in the current 
                                // node's descendants.
                                if forest
                                    .neighbors_directed(visited_node_idx, Direction::Outgoing)
                                    .count()
                                    == 0
                                {
                                    parents.insert(visited_node_idx);
                                    for n in run_queue.iter() {
                                        if n.node_index.index() == visited_node_idx.index() {
                                            demoted_leaves.push(n.node_index);
                                        }
                                    }
                                }
                            }
                            Ordering::Equal => {
                                // There are no dependencies between current event and added event.
                                // Add dependencies from the parents of the visited node to the 
                                // added event.
                                for parent_idx in
                                    forest.neighbors_directed(visited_node_idx, Direction::Incoming)
                                {
                                    let parent_event =
                                        forest.node_weight(parent_idx).unwrap().as_ref().unwrap();
                                    if parent_event > &added_event {
                                        // The added event precedes the parent, so the parent
                                        // depends on the added event.
                                        parents.insert(parent_idx);
                                    }
                                }
                            }
                            Ordering::Greater => {
                                // The added event depends on the visited event.
                                children.insert(visited_node_idx);
                                preceding_events.insert(visited_node_idx);
                                // Add dependencies from the parents of the visited node to the 
                                // added event.  Also, note edges that become redundant for 
                                // removal.
                                for parent_idx in
                                    forest.neighbors_directed(visited_node_idx, Direction::Incoming)
                                {
                                    let parent_event =
                                        forest.node_weight(parent_idx).unwrap().as_ref().unwrap();
                                    if parent_event > &added_event {
                                        // The added event precedes the parent, so the parent
                                        // depends on the added event.
                                        parents.insert(parent_idx);
                                        // Edge from parent to visited node becomes redundant.
                                        let redundant_edge =
                                            forest.find_edge(parent_idx, visited_node_idx).unwrap();
                                        redundant_edges.push(redundant_edge);
                                    }
                                }
                            }
                        };
                    } else {
                        // Reached a node that is already executing, but hasn't been completed.
                        // The current node will probably get added as a leaf. Add dependencies
                        // between parents and current event.
                        for parent in
                            forest.neighbors_directed(visited_node_idx, Direction::Incoming)
                        {
                            let parent_node = forest.node_weight(parent).unwrap().as_ref().unwrap();
                            if parent_node > &added_event {
                                parents.insert(parent);
                            }
                        }
                    }
                }
            }

            // Add the node into the forest.
            let event_timestamp: Timestamp = added_event.timestamp.clone();
            let event_idx: NodeIndex<u32> = forest.add_node(Some(added_event));

            // Add edges indicating dependencies.
            for child in children {
                forest.add_edge(event_idx, child, ());
            }
            for parent in parents {
                forest.add_edge(parent, event_idx, ());
            }

            // Remove redundant edges
            for redundant_edge in redundant_edges {
                forest.remove_edge(redundant_edge).unwrap();
            }

            // Clean up the leaves and the run queue, if any.
            // TODO (Sukrit) :: BinaryHeap does not provide a way to remove an element that is not 
            // at the top of the heap. So, this particularly costly implementation clones the 
            // elements out of the earlier run_queue, clears the run_queue and initializes it 
            // afresh with the set difference of the old run_queue and the nodes to remove.
            // Since the invocation of this code is hopefully rare, we can optimize it later.
            if demoted_leaves.len() > 0 {
                leaves.retain(|event| !demoted_leaves.contains(&event.node_index));
                // Reconstruct the run queue.
                let old_run_queue: Vec<RunnableEvent> = run_queue.drain().collect();
                for event in old_run_queue {
                    if !demoted_leaves.contains(&event.node_index) {
                        run_queue.push(event);
                    }
                }
            }

            // If the added event depends on no others, then we can safely create a new leaf in the 
            // forest and add the event to the run queue.
            if preceding_events.is_empty() {
                leaves.push(RunnableEvent::new(event_idx).with_timestamp(event_timestamp.clone()));
                run_queue.push(RunnableEvent::new(event_idx).with_timestamp(event_timestamp));
            }
        }

        if forest.node_count() > 100 {
            slog::warn!(
                crate::TERMINAL_LOGGER,
                "{} operator events queued in lattice. Increase number of operator executors or \
                decrease incoming message frequency to reduce load.", forest.node_count()
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
        let _leaves = self.leaves.lock().await;
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

    /// Mark an event as completed, and break the dependency from this event to its parents.
    ///
    /// `event_id` is the unique identifer returned by the [`ExecutionLattice::get_event`]
    /// invocation.
    pub async fn mark_as_completed(&self, event_id: usize) {
        // Take locks over everything.
        let mut forest = self.forest.lock().await;
        let mut leaves = self.leaves.lock().await;
        let mut run_queue = self.run_queue.lock().await;

        let node_idx: NodeIndex<u32> = NodeIndex::new(event_id);

        // Throw an error if the item was not in the leaves.
        let event = RunnableEvent::new(node_idx);
        let event_idx = leaves
            .iter()
            .position(|e| e == &event)
            .expect("Item must be in the leaves of the lattice.");
        leaves.remove(event_idx);

        // Capture the parents of the node.
        let parent_ids: Vec<NodeIndex> = forest
            .neighbors_directed(node_idx, Direction::Incoming)
            .collect();

        // Remove the node from the graph. This will also remove edges from the parents.
        forest.remove_node(node_idx);

        // Promote parents to leaves if they have no dependencies, and add their corresponding
        // events to the run queue.
        for parent_id in parent_ids {
            if forest
                .neighbors_directed(parent_id, Direction::Outgoing)
                .count()
                == 0
            {
                let timestamp: Timestamp = forest[parent_id].as_ref().unwrap().timestamp.clone();
                let parent = RunnableEvent::new(parent_id).with_timestamp(timestamp);
                leaves.push(parent.clone());
                run_queue.push(parent);
            }
        }
    }

    /// Convert graph to string in DOT format.
    #[allow(dead_code)]
    pub async fn to_dot(&self) -> String {
        // Lock the graph.
        let forest = self.forest.lock().await;
        let leaves = self.leaves.lock().await;
        let graph = forest.map(
            |nx, n| {
                n.as_ref().map_or_else(
                    || {
                        leaves
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

impl fmt::Debug for ExecutionLattice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExecutionLattice")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataflow::Timestamp;
    use futures::executor::block_on;

    /// Test that a leaf gets added correctly to an empty lattice and that we can retrieve it from
    /// the lattice.
    #[test]
    fn test_leaf_addition() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![OperatorEvent::new(
            Timestamp::Time(vec![1]),
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
            event.timestamp,
            Timestamp::Time(vec![1 as u64]),
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
                Timestamp::Time(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![1]),
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
            event.timestamp,
            Timestamp::Time(vec![1 as u64]),
            "The wrong event was returned by the lattice."
        );

        // Check that the other event is returned without marking the first one as completed.
        // This shows that they can be executed concurrently.
        let (event_2, _event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp,
            Timestamp::Time(vec![1 as u64]),
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
                Timestamp::Time(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![1]),
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
            event.timestamp == Timestamp::Time(vec![1 as u64]) && !event.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );

        // Check that the first event is returned correctly by the lattice.
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_2.timestamp == Timestamp::Time(vec![1 as u64]) && !event.is_watermark_callback,
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
            event_3.timestamp == Timestamp::Time(vec![1 as u64]) && event_3.is_watermark_callback,
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
                Timestamp::Time(vec![3]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![2]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![1]),
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
            event.timestamp,
            Timestamp::Time(vec![1 as u64]),
            "The wrong event was returned by the lattice."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id));
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp,
            Timestamp::Time(vec![2 as u64]),
            "The wrong event was returned by the lattice."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_2));
        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_3.timestamp,
            Timestamp::Time(vec![3 as u64]),
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
                Timestamp::Time(vec![3]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![2]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![1]),
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
            event.timestamp,
            Timestamp::Time(vec![1 as u64]),
            "The wrong event was returned by the lattice."
        );
        let (event_2, _event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp,
            Timestamp::Time(vec![2 as u64]),
            "The wrong event was returned by the lattice."
        );
        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_3.timestamp,
            Timestamp::Time(vec![3 as u64]),
            "The wrong event was returned by the lattice."
        );
    }

    /// Test that concurrent messages are followed by their watermarks.
    #[test]
    fn test_concurrent_messages_watermarks_diff_timestamps() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        let events = vec![
            OperatorEvent::new(
                Timestamp::Time(vec![3]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![2]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![1]),
                true,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![1]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![2]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || (),
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![3]),
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
            event.timestamp == Timestamp::Time(vec![1 as u64]) && !event.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_2.timestamp == Timestamp::Time(vec![2 as u64]) && !event_2.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        let (event_3, event_id_3) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_3.timestamp == Timestamp::Time(vec![3 as u64]) && !event_3.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id));
        let (event_4, event_id_4) = block_on(lattice.get_event()).unwrap();
        assert!(
            event_4.timestamp == Timestamp::Time(vec![1 as u64]) && event_4.is_watermark_callback,
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
            event_5.timestamp == Timestamp::Time(vec![2 as u64]) && event_5.is_watermark_callback,
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
            event_6.timestamp == Timestamp::Time(vec![3 as u64]) && event_6.is_watermark_callback,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_6));
        assert!(
            block_on(lattice.get_event()).is_none(),
            "The wrong event was returned by the lattice."
        );
    }

    /// Tests that duplicate events do not end up in the lattice's leaves or
    /// run queue. This can happen if duplicate edges exist in the dependency
    /// graph.
    #[test]
    fn test_no_duplicates() {
        let lattice = ExecutionLattice::new();
        // Add 2 operators that can run concurrently.
        let initial_events = vec![
            OperatorEvent::new(
                Timestamp::Time(vec![0]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || {},
            ),
            OperatorEvent::new(
                Timestamp::Time(vec![0]),
                false,
                0,
                HashSet::new(),
                HashSet::new(),
                || {},
            ),
        ];
        block_on(lattice.add_events(initial_events));

        // Generate events A and B where B precedes A.
        let event_a = OperatorEvent::new(
            Timestamp::Time(vec![0]),
            true,
            20,
            HashSet::new(),
            HashSet::new(),
            || {},
        );
        let event_b = OperatorEvent::new(
            Timestamp::Time(vec![0]),
            true,
            0,
            HashSet::new(),
            HashSet::new(),
            || {},
        );
        assert!(event_a > event_b, "Event B must precede event A.");

        // Insert events in reverse order. Due to how the traversal of the
        // dependency graph is performed, this can result in duplicate edges
        // when using vectors instead of sets to store an inserted event's
        // parents and children. Duplicate edges may result in duplicate
        // attempts to run the same event.
        block_on(lattice.add_events(vec![event_a]));
        block_on(lattice.add_events(vec![event_b]));
        // Dependency graph should be:
        //        -> C
        // A -> B
        //        -> D

        // Run events C and D
        let (event_1, event_1_id) = block_on(lattice.get_event()).unwrap();
        let (event_2, event_2_id) = block_on(lattice.get_event()).unwrap();
        assert!(
            !event_1.is_watermark_callback,
            "Should process events C and D before watermark callbacks."
        );
        assert!(
            !event_2.is_watermark_callback,
            "Should process events C and D before watermark callbacks."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "No other events should run until C and D complete."
        );
        block_on(lattice.mark_as_completed(event_1_id));
        assert!(
            block_on(lattice.get_event()).is_none(),
            "No other events should run until C and D complete."
        );
        block_on(lattice.mark_as_completed(event_2_id));

        // Run event B.
        let (event_b, event_b_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_b.priority, 0,
            "Event B should run after events C and D."
        );
        assert!(
            block_on(lattice.get_event()).is_none(),
            "A should not run until B completes."
        );
        block_on(lattice.mark_as_completed(event_b_id));

        // Run event A.
        let (_event_a, event_a_id) = block_on(lattice.get_event()).unwrap();
        block_on(lattice.mark_as_completed(event_a_id));

        // No more events should be in the lattice.
        assert!(
            block_on(lattice.get_event()).is_none(),
            "There should be no more events in the lattice."
        );
    }
}
