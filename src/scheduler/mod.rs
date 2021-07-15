use crate::dataflow::graph::{Channel, Graph, Vertex};

// Crate-wide visible submodules
pub(crate) mod endpoints_manager;

// Public exports
pub mod channel_manager;

/// Schedules a dataflow graph. Assigns operators to nodes and updates channels, deciding what
/// type of channels (e.g., `Channel::InterThread`, `Channel::InterNode`) to use depending on
/// the location of the operators. After running this method, there should be no unscheduled
/// channels remaining.
pub(crate) fn schedule(graph: &Graph) -> Graph {
    let mut scheduled_graph = graph.clone();
    for stream in scheduled_graph.get_streams_ref_mut() {
        let source_node_id = match stream.source() {
            Vertex::Driver(node_id) => node_id,
            Vertex::Operator(operator_id) => graph.get_operator(operator_id).unwrap().node_id,
        };

        let mut channels = Vec::new();
        for channel in stream.get_channels() {
            channels.push(match channel {
                Channel::Unscheduled(cm) => {
                    let sink_node_id = match cm.sink {
                        Vertex::Driver(node_id) => node_id,
                        Vertex::Operator(operator_id) => {
                            graph.get_operator(operator_id).unwrap().node_id
                        }
                    };
                    if source_node_id == sink_node_id {
                        Channel::InterThread(cm)
                    } else {
                        Channel::InterNode(cm)
                    }
                }
                c => c,
            });
        }
        stream.set_channels(channels);
    }
    scheduled_graph
}
