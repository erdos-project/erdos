#[macro_use]
extern crate criterion;
extern crate erdos;

use criterion::Criterion;
use std::sync::Arc;
use std::thread;

use erdos::communication::{Pusher, RecvEndpoint, SendEndpoint};
use erdos::dataflow::stream::{ReadStream, WriteStream};
use erdos::dataflow::{Message, MessageWrapper, Timestamp};
use erdos::node::node::{Node, NodeConfig};
use erdos::node::operator_executor::{OperatorExecutor, OperatorExecutorStream};

fn inter_thread_msg_throughput(num_msgs: usize, vec_len: usize) {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut send_endpoint = SendEndpoint::InterThread(tx);
    let mut recv_endpoint = RecvEndpoint::InterThread(rx);

    let num_sent_msgs = num_msgs;
    let sender_handle = thread::spawn(move || {
        let mut msg = Message {
            timestamp: Timestamp { time: vec![1] },
            data: vec![0; vec_len],
        };
        for _cnt in 0..num_sent_msgs {
            let cur_msg = msg.clone();
            send_endpoint.send(cur_msg);
        }
    });

    let num_recv_msgs = num_msgs;
    let receiver_handle = thread::spawn(move || {
        for _cnt in 0..num_recv_msgs {
            recv_endpoint.read().unwrap();
        }
    });

    sender_handle.join().unwrap();
    receiver_handle.join().unwrap();
}

fn inter_process_msg_throughput(num_msgs: usize) {
    let addresses = vec![
        "127.0.0.1:9001".parse().unwrap(),
        "127.0.0.1:9002".parse().unwrap(),
    ];
    let node1_config = NodeConfig::new(0, addresses.clone(), 1, 2);
    let mut node1 = Node::new(node1_config);
    let node2_config = NodeConfig::new(1, addresses.clone(), 1, 2);
    let mut node2 = Node::new(node2_config);

    let mut ws: WriteStream<Vec<u8>> = WriteStream::new();

    let op1 = OperatorExecutor::new(vec![]);
    //    let op1_handle = thread::spawn(move || op1.execute());

    // Channel from operator running on node 1 to sender threads.
    let (tx_node_1, rx_node_1): (
        tokio::sync::mpsc::UnboundedSender<MessageWrapper<Vec<u8>>>,
        tokio::sync::mpsc::UnboundedReceiver<MessageWrapper<Vec<u8>>>,
    ) = tokio::sync::mpsc::unbounded_channel();
    ws.add_send_endpoint(SendEndpoint::InterProcess(tx_node_1));

    // Channel from recv threads to operator running on node 2.
    let (tx_recv_node_2, rx_op_node_2): (
        std::sync::mpsc::Sender<MessageWrapper<Vec<u8>>>,
        std::sync::mpsc::Receiver<MessageWrapper<Vec<u8>>>,
    ) = std::sync::mpsc::channel();

    let mut rs = ReadStream::from_write_stream(ws);
    rs.add_receive_endpoint(RecvEndpoint::InterThread(rx_op_node_2));

    let pusher_node_2 = Pusher {
        endpoints: vec![SendEndpoint::InterThread(tx_recv_node_2)],
    };
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("inter_thrd_msg_thrghput 100000 1", |b| {
        b.iter(|| inter_thread_msg_throughput(100000, 1))
    });
    c.bench_function("inter_thrd_msg_thrghput 1000 10000", |b| {
        b.iter(|| inter_thread_msg_throughput(1000, 10000))
    });
    c.bench_function("inter_proc_msg_thrghput 1000 1", |b| {
        b.iter(|| inter_process_msg_throughput(1000))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
