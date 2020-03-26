// Export the modules to be visible outside of the dataflow module.
pub mod callback_builder;
pub mod graph;
pub mod message;
pub mod operator;
pub mod operators;
pub mod state;
pub mod stream;

// Re-export structs as if they were defined here.
pub use message::{Data, Message, Timestamp, TimestampedData};
pub use operator::{Operator, OperatorConfig};
pub use state::State;
pub use stream::{
    EventMakerT, LoopStream, ReadStream, StatefulReadStream, WriteStream, WriteStreamError,
};

#[cfg(test)]
mod tests {
    // Imports used in tests
    use std::{cell::RefCell, rc::Rc};
    use tokio::sync::mpsc;

    use crate::communication::SendEndpoint;
    use crate::dataflow::{
        callback_builder::MultiStreamEventMaker,
        stream::{
            EventMakerT, InternalReadStream, ReadStream, StreamId, WriteStream, WriteStreamT,
        },
        Message, Timestamp, TimestampedData,
    };

    // Tests if the `EventMakerT` creates a callback event.
    #[test]
    fn test_callback() {
        let rs: ReadStream<String> = ReadStream::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        rs.add_callback(move |_t: Timestamp, msg: String| {
            tx.send(msg).unwrap();
        });

        // Generate events from message
        let msg = Message::new_message(Timestamp::new(vec![1]), String::from("msg 1"));
        let irs: Rc<RefCell<InternalReadStream<String>>> = (&rs).into();
        let mut events = irs.borrow().make_events(msg);
        assert!(events.len() == 1);

        // Invoke callback
        match events.pop() {
            Some(event) => {
                (event.callback)();
                assert_eq!(rx.try_recv().unwrap(), "msg 1");
            }
            None => unreachable!(),
        }
    }

    // Tests if the `EventMakerT` creates a watermark callback event.
    #[test]
    fn test_watermark_callback() {
        // Setup: generate ReadStream with 1 callback
        let rs: ReadStream<String> = ReadStream::new();
        let irs: Rc<RefCell<InternalReadStream<String>>> = (&rs).into();
        let (tx, mut rx) = mpsc::unbounded_channel();
        rs.add_watermark_callback(move |_timestamp: &Timestamp| {
            tx.send("received watermark").unwrap();
        });

        // Generate events from message
        let msg = Message::new_message(Timestamp::new(vec![1]), String::from("msg 1"));
        let watermark_msg = Message::new_watermark(Timestamp::new(vec![2]));
        // Non-watermark messages should not create events
        let events = irs.borrow().make_events(msg);
        assert!(events.is_empty());
        // Watermark messages should create events
        let mut events = irs.borrow().make_events(watermark_msg);
        assert!(events.len() == 1);

        // Invoke callback
        match events.pop() {
            Some(event) => {
                (event.callback)();
                assert_eq!(rx.try_recv().unwrap(), "received watermark");
            }
            None => unreachable!(),
        }
    }

    #[derive(Clone)]
    struct CounterState {
        count: usize,
    }

    // Tests if the `EventMakerT` creates a callback event on a stateful read stream.
    #[test]
    fn test_stateful_callback() {
        // Setup: generate StatefulReadStream with 1 callback
        let rs: ReadStream<usize> = ReadStream::new();
        let state = CounterState { count: 0 };
        let srs = rs.add_state(state);
        let irs: Rc<RefCell<InternalReadStream<usize>>> = (&rs).into();
        srs.add_callback(|_t: Timestamp, data: usize, state: &mut CounterState| {
            state.count += data
        });

        // Generate events from message
        let msg = Message::new_message(Timestamp::new(vec![1]), 42);
        let mut events = irs.borrow().make_events(msg);
        assert!(events.len() == 1);

        // Invoke callback
        match events.pop() {
            Some(event) => {
                (event.callback)();
                assert_eq!(srs.get_state().count, 42);
            }
            None => unreachable!(),
        }
    }

    // Tests if the `EventMakerT` creates a watermark callback event on a stateful read stream.
    #[test]
    fn test_stateful_watermark_callback() {
        // Setup: generate StatefulReadStream with 1 watermark callback
        let rs: ReadStream<usize> = ReadStream::new();
        let srs = rs.add_state(CounterState { count: 0 });
        let irs: Rc<RefCell<InternalReadStream<usize>>> = (&rs).into();
        srs.add_watermark_callback(|_timestamp: &Timestamp, state: &mut CounterState| {
            state.count += 42;
        });

        // Generate events from message
        let msg = Message::new_message(Timestamp::new(vec![1]), 1);
        let watermark_msg = Message::new_watermark(Timestamp::new(vec![2]));
        // Non-watermark messages should not create events
        let events = irs.borrow().make_events(msg);
        assert!(events.is_empty());
        // Watermark messages should create events
        let mut events = irs.borrow().make_events(watermark_msg);
        assert!(events.len() == 1);

        // Invoke callback
        match events.pop() {
            Some(event) => {
                (event.callback)();
                assert_eq!(srs.get_state().count, 42);
            }
            None => unreachable!(),
        }
    }

    #[test]
    fn test_multi_stream_callback() {
        // Setup: generate 2 StatefulReadStream with 1 watermark callback across both
        let rs1: ReadStream<usize> = ReadStream::new();
        let srs1 = rs1.add_state(CounterState { count: 1 });
        let irs1: Rc<RefCell<InternalReadStream<usize>>> = (&rs1).into();
        let rs2: ReadStream<usize> = ReadStream::new();
        let srs2 = rs2.add_state(CounterState { count: 2 });
        let irs2: Rc<RefCell<InternalReadStream<usize>>> = (&rs2).into();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let cb = move |_t: &Timestamp, s1: &CounterState, s2: &CounterState| {
            tx.send(s1.count + s2.count).unwrap();
        };
        crate::add_watermark_callback!((srs1, srs2), (), (cb));

        // Generate events from message
        let msg1 = Message::new_message(Timestamp::new(vec![1]), 1);
        let msg2 = Message::new_message(Timestamp::new(vec![1]), 1);
        let watermark_msg1 = Message::new_watermark(Timestamp::new(vec![2]));
        let watermark_msg2 = Message::new_watermark(Timestamp::new(vec![2]));
        // Non-watermark messages should not create events
        let events = irs1.borrow().make_events(msg1);
        assert!(events.is_empty());
        let events = irs2.borrow().make_events(msg2);
        assert!(events.is_empty());
        // Watermark message on 1 stream should not create events
        let events = irs1.borrow().make_events(watermark_msg1);
        assert!(events.is_empty());
        // Watermark message on 2 streams should create events
        let mut events = irs2.borrow().make_events(watermark_msg2);
        assert!(events.len() == 1);

        // Invoke callback
        match events.pop() {
            Some(event) => {
                (event.callback)();
                assert_eq!(rx.try_recv().unwrap(), 3);
            }
            None => unreachable!(),
        }
    }

    #[test]
    fn test_multi_stream_stateful_callback() {
        // Setup: generate 3 StatefulReadStream with 1 watermark callback across both
        let rs1: ReadStream<usize> = ReadStream::new();
        let srs1 = rs1.add_state(CounterState { count: 1 });
        let irs1: Rc<RefCell<InternalReadStream<usize>>> = (&rs1).into();
        let rs2: ReadStream<usize> = ReadStream::new();
        let srs2 = rs2.add_state(CounterState { count: 2 });
        let irs2: Rc<RefCell<InternalReadStream<usize>>> = (&rs2).into();
        let rs3: ReadStream<usize> = ReadStream::new();
        let srs3 = rs3.add_state(CounterState { count: 3 });
        let irs3: Rc<RefCell<InternalReadStream<usize>>> = (&rs3).into();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let cb = move |_t: &Timestamp,
                       state: &mut CounterState,
                       s1: &CounterState,
                       s2: &CounterState,
                       s3: &CounterState| {
            state.count = s1.count + s2.count + s3.count;
            tx.send(state.count).unwrap();
        };

        // add_watermark_callback!((srs1, srs2, srs3), (), (cb), CounterState { count: 0 });
        srs1.add_read_stream(&srs2)
            .borrow_mut()
            .add_read_stream(&srs3)
            .borrow_mut()
            .add_state(CounterState { count: 0 })
            .borrow_mut()
            .add_watermark_callback(cb);

        // Generate events from message
        let msg1 = Message::new_message(Timestamp::new(vec![1]), 1);
        let msg2 = Message::new_message(Timestamp::new(vec![1]), 1);
        let msg3 = Message::new_message(Timestamp::new(vec![1]), 1);
        let watermark_msg1 = Message::new_watermark(Timestamp::new(vec![2]));
        let watermark_msg2 = Message::new_watermark(Timestamp::new(vec![2]));
        let watermark_msg3 = Message::new_watermark(Timestamp::new(vec![2]));
        // Non-watermark messages should not create events
        let events = irs1.borrow().make_events(msg1);
        assert!(events.is_empty());
        let events = irs2.borrow().make_events(msg2);
        assert!(events.is_empty());
        let events = irs2.borrow().make_events(msg3);
        assert!(events.is_empty());
        // Watermark message on 1 stream should not create events
        let events = irs1.borrow().make_events(watermark_msg1);
        assert!(events.is_empty());
        // Watermark message on 2 streams should not create events
        let events = irs2.borrow().make_events(watermark_msg2);
        assert!(events.is_empty());
        // Watermark message on 3 streams should create events
        let mut events = irs3.borrow().make_events(watermark_msg3);
        assert!(events.len() == 1);

        // Invoke callback
        match events.pop() {
            Some(event) => {
                (event.callback)();
                assert_eq!(rx.try_recv().unwrap(), 6);
            }
            None => unreachable!(),
        }
    }

    #[test]
    fn test_one_read_one_write_callback() {
        // Setup: generate OneReadOneWrite stream with 1 watermark callback.
        let rs: ReadStream<usize> = ReadStream::new();
        let state = CounterState { count: 5 };
        let srs = rs.add_state(state);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let endpoints = vec![SendEndpoint::InterThread(tx)];
        let ws: WriteStream<usize> =
            WriteStream::from_endpoints(endpoints, StreamId::new_deterministic());
        let rws = srs.add_write_stream(&ws);
        rws.borrow_mut().add_watermark_callback(
            |_t: &Timestamp, state: &CounterState, output_stream: &mut WriteStream<usize>| {
                let msg = TimestampedData {
                    timestamp: Timestamp::new(vec![1]),
                    data: state.count,
                };
                output_stream.send(Message::TimestampedData(msg)).unwrap()
            },
        );

        // Watermark messages should create events
        let t0 = Timestamp::new(vec![0]);
        let t1 = Timestamp::new(vec![1]);
        let t2 = Timestamp::new(vec![2]);

        // First watermark should create events
        let events = rws.borrow_mut().receive_watermark(rs.get_id(), t1);
        assert!(events.len() == 1);

        // Older watermarks should not create events
        let events = rws.borrow_mut().receive_watermark(rs.get_id(), t0);
        assert!(events.is_empty());

        // Newer watermarks should create events
        let mut events = rws.borrow_mut().receive_watermark(rs.get_id(), t2);
        assert!(events.len() == 1);

        // Invoke callback.
        match events.pop() {
            Some(event) => {
                (event.callback)();
                match rx.try_recv().unwrap() {
                    Message::TimestampedData(msg) => assert_eq!(msg.data, 5),
                    _ => unreachable!(),
                }
            }
            None => unreachable!(),
        }
    }

    #[test]
    fn test_callback_and_watermark_callback() {
        let rs: ReadStream<String> = ReadStream::new();
        let irs: Rc<RefCell<InternalReadStream<String>>> = (&rs).into();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        rs.add_callback(move |_t: Timestamp, _msg: String| {
            tx1.send("callback invoked").unwrap();
        });
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        rs.add_watermark_callback(move |_timestamp: &Timestamp| {
            tx2.send("watermark callback invoked").unwrap();
        });
        // Generate events from message
        let msg = Message::new_message(Timestamp::new(vec![1]), String::from(""));
        let watermark_msg = Message::new_watermark(Timestamp::new(vec![2]));
        // Non-watermark messages should not create events
        let mut events = irs.borrow().make_events(msg);
        assert!(events.len() == 1);
        match events.pop() {
            Some(event) => {
                (event.callback)();
                assert_eq!(rx1.try_recv().unwrap(), "callback invoked");
            }
            None => unreachable!(),
        }
        // Watermark messages should create events
        let mut events = irs.borrow().make_events(watermark_msg);
        assert!(events.len() == 1);
        // Invoke callback
        match events.pop() {
            Some(event) => {
                (event.callback)();
                assert_eq!(rx2.try_recv().unwrap(), "watermark callback invoked");
            }
            None => unreachable!(),
        }
    }
}
