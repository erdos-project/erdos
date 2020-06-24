use crate::dataflow::Timestamp;
use crate::node::operator_event::OperatorEvent;
use futures::lock::Mutex;
use petgraph::{
    stable_graph::{EdgeIndex, NodeIndex, StableGraph},
    visit::DfsPostOrder,
    Direction,
};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::fmt;
use std::sync::Arc;

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

/// `ExecutionLattice` is a data structure that maintains [`OperatorEvent`]s in a directed
/// acyclic graph according the partial order defined.
///
/// Events can be added to the lattice using the `add_event` function, and retrieved using the
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
///     lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), true, || ())).await;
///     lattice.add_event(OperatorEvent::new(Timestamp::new(vec![2]), true, || ())).await;
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
    /// events.
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

    /// Add an event to the lattice.
    ///
    /// This function moves the passed event into the lattice, and inserts the appropriate edges to
    /// existing events in the graph based on the partial order defined in [`OperatorEvent`].
    pub async fn add_event(&self, event: OperatorEvent) {
        // Take locks over everything.
        let mut forest = self.forest.lock().await;
        let mut roots = self.roots.lock().await;
        let mut run_queue = self.run_queue.lock().await;

        // Iterate through all the roots, and figure out where to add a dependency edge.
        let mut dfs = DfsPostOrder::empty(&*forest);
        let mut dependencies: HashSet<NodeIndex<u32>> = HashSet::new();
        let mut add_edges_to: Vec<NodeIndex<u32>> = Vec::new();
        let mut add_edges_from: Vec<NodeIndex<u32>> = Vec::new();
        let mut remove_roots: Vec<RunnableEvent> = Vec::new();

        for root in roots.iter() {
            // Retrieve the index into the lattice.
            let root_index: NodeIndex<u32> = root.node_index;

            // This function maintains the stack when iterating over the graph so we do not have to
            // check the same node in the graph twice.
            dfs.move_to(root_index);

            // Retrieve the next node in DFS order from the forest.
            while let Some(nx) = dfs.next(&*forest) {
                // Check if any of the children of the current node in DFS traversal already have a
                // dependency on the node we are trying to add.
                let mut dependent_child: bool = false;
                for child in forest.neighbors(nx) {
                    if dependencies.contains(&child) {
                        dependencies.insert(nx);
                        dependent_child = true;
                        break;
                    }
                }

                // If the event we are trying to add has already been added as a dependency of one
                // of the children of the DFS node, then move to the next root of the forest.
                if dependent_child {
                    break;
                }

                if let Some(node) = forest.node_weight(nx).unwrap().as_ref() {
                    // If the event we are trying to add is dependent on the DFS node, then
                    // add an edge from the DFS node to the event to be added.
                    if &event <= node {
                        // Check if any of the children of the DFS node should be dependent on the
                        // event we are trying to add.
                        let mut remove_edges: Vec<NodeIndex<u32>> = Vec::new();
                        for child in forest.neighbors(nx) {
                            let child_node: &OperatorEvent =
                                forest.node_weight(child).unwrap().as_ref().unwrap();
                            if child_node < &event {
                                // The child has a dependency on the node being added.
                                // Break the dependency from the DFS node to its child, and add a
                                // dependency from the node to be added to the child.
                                add_edges_to.push(child);
                                remove_edges.push(child);
                            }
                        }

                        // Add a dependency from the DFS node to the node being added, only if the
                        // partial order says so.
                        if &event < node {
                            add_edges_from.push(nx);
                            for child in remove_edges {
                                let edge = forest.find_edge(nx, child).unwrap();
                                forest.remove_edge(edge);
                            }
                            dependencies.insert(nx);
                        }
                    } else {
                        // The current event had no dependent children, but the node in DFS is
                        // dependent on the current event. Usually, this is resolved in a level
                        // above, but add edges if the node is root.
                        if forest.neighbors_directed(nx, Direction::Incoming).count() == 0 {
                            add_edges_to.push(nx);
                            for n in run_queue.iter() {
                                if n.node_index.index() == nx.index() {
                                    remove_roots.push(n.clone());
                                }
                            }
                        }
                    }
                } else {
                    // Reached a node that is already executing, but hasn't been completed.
                    // The current node will probably get added as a root. Add dependencies to the
                    // children, if any.
                    for child in forest.neighbors(nx) {
                        let child_node = forest.node_weight(child).unwrap().as_ref().unwrap();
                        if child_node < &event {
                            add_edges_to.push(child);
                        }
                    }
                }
            }
        }

        // Add the node into the forest.
        let event_timestamp: Timestamp = event.timestamp.clone();
        let event_ix: NodeIndex<u32> = forest.add_node(Some(event));

        // Add the edges from the inserted event to its dependencies.
        for child in add_edges_to {
            forest.add_edge(event_ix, child, ());
        }

        // Break the edges from the dependency graph.
        for node in add_edges_from {
            forest.add_edge(node, event_ix, ());
        }

        // Clean up the roots and the run queue, if any.
        // TODO (Sukrit) :: BinaryHeap does not provide a way to remove an element that is not at
        // the top of the heap. So, this particularly costly implementation clones the elements out
        // of the earlier run_queue, clears the run_queue and initializes it afresh with the set
        // difference of the old run_queue and the nodes to remove.
        // Since the invocation of this code is hopefully rare, we can optimize it later.
        if remove_roots.len() > 0 {
            // The run queue needs to be reconstructed.
            let mut old_run_queue: Vec<RunnableEvent> = Vec::new();
            for event in run_queue.iter() {
                old_run_queue.push(event.clone());
            }
            run_queue.clear();

            roots.retain(|e| !remove_roots.contains(e));

            for item in old_run_queue {
                if !remove_roots.contains(&item) {
                    run_queue.push(item);
                }
            }
        }

        // If no dependencies of this event, then we can safely create a new root in the forest and
        // add the event to the run queue.
        if dependencies.is_empty() {
            roots.push(RunnableEvent::new(event_ix).with_timestamp(event_timestamp.clone()));
            run_queue.push(RunnableEvent::new(event_ix).with_timestamp(event_timestamp));
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

        let node_id: NodeIndex<u32> = NodeIndex::new(event_id);

        // Throw an error if the item was not in the roots.
        let event = RunnableEvent::new(node_id);
        let event_idx = roots
            .iter()
            .position(|e| e == &event)
            .expect("Item must be in the roots of the lattice.");
        roots.remove(event_idx);

        // Go over the children of the node, and check which ones have no dependencies on other
        // nodes, and add them to the list of the roots.
        let mut remove_edges: Vec<NodeIndex<u32>> = Vec::new();
        let mut root_nodes: Vec<RunnableEvent> = Vec::new();
        for child_id in forest.neighbors(node_id) {
            // Does this node have any other incoming edge?
            if forest
                .neighbors_directed(child_id, Direction::Incoming)
                .count()
                == 1
            {
                let timestamp: Timestamp = forest[child_id].as_ref().unwrap().timestamp.clone();
                root_nodes.push(RunnableEvent::new(child_id).with_timestamp(timestamp));
            }

            // Remove the edge from the child.
            remove_edges.push(child_id);
        }

        // Remove the edges from the forest.
        for child_id in remove_edges {
            let edge_id: EdgeIndex<u32> = forest.find_edge(node_id, child_id).unwrap();
            forest.remove_edge(edge_id);
        }

        // Push the root nodes to the root of the forest, and add them to the run queue.
        for child in root_nodes {
            roots.push(child.clone());
            run_queue.push(child);
        }

        // Remove the node from the forest.
        forest.remove_node(node_id);
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
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), false, || ())));

        // Ensure that the correct event is returned by the lattice.
        let (event, _event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0], 1,
            "The wrong event was returned by the lattice."
        );

        // Ensure that only one event is returned by the lattice.
        let next_event = block_on(lattice.get_event());
        assert_eq!(
            next_event.is_none(),
            true,
            "Expected no event from the lattice"
        );
    }

    /// Test that the addition of two messages of the same timestamp leads to no dependencies.
    #[test]
    fn test_concurrent_messages() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), false, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), false, || ())));

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
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), false, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), false, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), true, || ())));

        // Check that the first event is returned correctly by the lattice.
        let (event, event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0] == 1 && !event.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );

        // Check that the first event is returned correctly by the lattice.
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp.time[0] == 1 && !event.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
        let no_event = block_on(lattice.get_event());
        assert_eq!(
            no_event.is_none(),
            true,
            "Expected no event from the lattice"
        );

        // Mark one of the event as completed, and still don't expect an event.
        block_on(lattice.mark_as_completed(event_id));

        let no_event_2 = block_on(lattice.get_event());
        assert_eq!(
            no_event_2.is_none(),
            true,
            "Expected no event from the lattice"
        );

        // Mark the other as completed and expect a Watermark.
        block_on(lattice.mark_as_completed(event_id_2));

        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_3.timestamp.time[0] == 1 && event_3.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
    }

    /// Test that the addition of three watermark messages in reverse order, leads to them being
    /// executed in the correct order.
    #[test]
    fn test_unordered_watermark() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![3]), true, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![2]), true, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), true, || ())));
        let (event, event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0], 1,
            "The wrong event was returned by the lattice."
        );
        assert_eq!(
            block_on(lattice.get_event()).is_none(),
            true,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id));
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp.time[0], 2,
            "The wrong event was returned by the lattice."
        );
        assert_eq!(
            block_on(lattice.get_event()).is_none(),
            true,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_2));
        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_3.timestamp.time[0], 3,
            "The wrong event was returned by the lattice."
        );
        assert_eq!(
            block_on(lattice.get_event()).is_none(),
            true,
            "The wrong event was returned by the lattice."
        );
    }

    /// Test that the addition of messages of different timestamps leads to concurrent execution.
    #[test]
    fn test_concurrent_messages_diff_timestamps() {
        let lattice: ExecutionLattice = ExecutionLattice::new();
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![3]), false, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![2]), false, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), false, || ())));
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
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![3]), true, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![2]), true, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), true, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), false, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![2]), false, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![3]), false, || ())));
        let (event, event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0] == 1 && !event.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
        let (event_2, event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp.time[0] == 2 && !event_2.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
        let (event_3, event_id_3) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_3.timestamp.time[0] == 3 && !event_3.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
        assert_eq!(
            block_on(lattice.get_event()).is_none(),
            true,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id));
        let (event_4, event_id_4) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_4.timestamp.time[0] == 1 && event_4.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
        assert_eq!(
            block_on(lattice.get_event()).is_none(),
            true,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_4));
        assert_eq!(
            block_on(lattice.get_event()).is_none(),
            true,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_2));
        let (event_5, event_id_5) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_5.timestamp.time[0] == 2 && event_5.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_3));
        assert_eq!(
            block_on(lattice.get_event()).is_none(),
            true,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_5));
        let (event_6, event_id_6) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_6.timestamp.time[0] == 3 && event_6.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
        block_on(lattice.mark_as_completed(event_id_6));
        assert_eq!(
            block_on(lattice.get_event()).is_none(),
            true,
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
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![2]), false, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![1]), true, || ())));
        block_on(lattice.add_event(OperatorEvent::new(Timestamp::new(vec![3]), false, || ())));

        let (event, _event_id) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event.timestamp.time[0] == 1 && event.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );

        let (event_2, _event_id_2) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_2.timestamp.time[0] == 2 && !event_2.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
        let (event_3, _event_id_3) = block_on(lattice.get_event()).unwrap();
        assert_eq!(
            event_3.timestamp.time[0] == 3 && !event_3.is_watermark_callback,
            true,
            "The wrong event was returned by the lattice."
        );
    }
}
