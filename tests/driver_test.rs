use erdos::dataflow::{
    graph::*,
    operators::{JoinOperator, SourceOperator},
    OperatorConfig,
};
use erdos::*;

/* TODO: fix these
#[test]
fn test_graph() {
    let ws1 = connect_1_write!(SourceOperator, OperatorConfig::new((), true, 0));
    let ws2 = connect_1_write!(SourceOperator, OperatorConfig::new((), true, 0));
    let _ws3 = connect_1_write!(JoinOperator<usize, usize, usize>, OperatorConfig::new((), true, 1), ws1, ws2);

    let graph: Graph = default_graph::clone();
    assert_eq!(graph.get_operators_on(0).len(), 2);
    assert_eq!(graph.get_operators_on(1).len(), 1);

    let mut operators = graph.get_operators_on(0);
    operators.append(&mut graph.get_operators_on(1));

    let channels: Vec<ChannelInfo> = operators
        .iter()
        .map(|op| graph.get_channels_to(op.id))
        .flatten()
        .collect();
    assert_eq!(channels.len(), 2);

    let source_ops: Vec<&OperatorInfo> = operators
        .iter()
        .filter(|op| op.read_stream_ids.is_empty())
        .collect();
    assert_eq!(source_ops.len(), 2);
    assert_eq!(source_ops[0].node_id, 0);
    assert_eq!(source_ops[1].node_id, 0);

    let join_op: Vec<&OperatorInfo> = operators
        .iter()
        .filter(|op| !op.read_stream_ids.is_empty())
        .collect();
    assert_eq!(join_op.len(), 1);
    let join_op = join_op[0];
    assert_eq!(join_op.node_id, 1);

    for source_op in source_ops {
        assert!(channels[0].source_id == source_op.id || channels[1].source_id == source_op.id);
    }
    assert!(channels[0].sink_id == join_op.id);
    assert!(channels[1].sink_id == join_op.id);
}
*/
