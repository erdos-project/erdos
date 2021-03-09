//! Streams are used to send data between operators.
//!
//! In the `Operator::connect` function, operators return the streams on which
//! they intend to send messages as [`WriteStream`]s.
//! Any number of operators can be connected to those streams to read
//! data via the `erdos::connect` macros; however, only the operator that
//! created the stream may send data.
//!
//! [`WriteStreamT::send`] broadcasts data to all connected operators, using
//! zero-copy communication for operators on the same node.
//! Messages sent across nodes are serialized using
//! [abomonation](https://github.com/TimelyDataflow/abomonation) if possible,
//! before falling back to [bincode](https://github.com/servo/bincode).
//!
//! The streams an operator reads from and writes to are automatically passed
//! to the `Operator::new` function.

use std::sync::Arc;

use crate::{
    dataflow::{Data, Message},
    node::operator_event::OperatorEvent,
};

// Private submodules
mod extract_stream;
mod ingest_stream;
mod internal_read_stream;
mod loop_stream;
mod read_stream;
mod write_stream;

// Public submodules
pub mod errors;

// Private imports
use errors::WriteStreamError;

// Public exports
pub use extract_stream::ExtractStream;
pub use ingest_stream::IngestStream;
#[doc(hidden)]
pub use internal_read_stream::InternalReadStream;
#[doc(hidden)]
pub use loop_stream::LoopStream;
pub use read_stream::ReadStream;
pub use write_stream::WriteStream;

pub type StreamId = crate::Uuid;

pub(crate) trait EventMakerT {
    type EventDataType: Data;

    /// Returns the id of the stream.
    fn get_id(&self) -> StreamId;

    /// Returns the vector of events that a message receipt generates.
    fn make_events(&self, msg: Arc<Message<Self::EventDataType>>) -> Vec<OperatorEvent>;
}

/// Write stream trait which allows specialized implementations of
/// [`send`](WriteStreamT::send) depending on the serialization library used.
pub trait WriteStreamT<D: Data> {
    /// Sends a messsage to a channel.
    fn send(&mut self, msg: Message<D>) -> Result<(), WriteStreamError>;
}

#[cfg(test)]
mod tests {
    use super::{WriteStream, WriteStreamT};
    use crate::communication::SendEndpoint;
    use crate::dataflow::{message::TimestampedData, stream::StreamId, Message, Timestamp};
    use std::thread;
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;

    pub fn make_default_runtime() -> Runtime {
        Builder::new()
            .basic_scheduler()
            .thread_name("erdos-test")
            .enable_all()
            .build()
            .unwrap()
    }

    /* Fails
    #[test]
    fn test_read_stream_sendable() {
        let rs: ReadStream<usize> = ReadStream::new();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let rs = rx.recv().unwrap();
        });
        tx.send(rs).unwrap();
    }
    */

    #[test]
    fn test_write_stream_sendable() {
        let mut rt = make_default_runtime();

        let ws: WriteStream<usize> = WriteStream::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(ws).unwrap();

        rt.block_on(rx.recv()).unwrap();
    }

    // Test that sends two messages on a write stream. It checks if both messages are received.
    #[test]
    fn test_write_stream_send() {
        let mut rt = make_default_runtime();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let endpoints = vec![SendEndpoint::InterThread(tx)];
        let mut ws: WriteStream<usize> =
            WriteStream::from_endpoints(endpoints, StreamId::new_deterministic());
        thread::spawn(move || {
            let msg1 = Message::TimestampedData(TimestampedData::new(Timestamp::new(vec![0]), 1));
            let msg2 = Message::TimestampedData(TimestampedData::new(Timestamp::new(vec![0]), 2));
            ws.send(msg1).unwrap();
            ws.send(msg2).unwrap();
        });
        let first_msg = rt.block_on(rx.recv()).unwrap();
        match &*first_msg {
            Message::TimestampedData(td) => {
                assert_eq!(td.data, 1);
            }
            _ => {
                panic!("Unexpected first message");
            }
        }
        let second_msg = rt.block_on(rx.recv()).unwrap();
        match &*second_msg {
            Message::TimestampedData(td) => {
                assert_eq!(td.data, 2);
            }
            _ => {
                panic!("Unexpected first message");
            }
        }
    }

    // Test that sends two watermarks on a stream. It checks that they are received in the same
    // order.
    #[test]
    fn test_write_stream_watermark() {
        let mut rt = make_default_runtime();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let endpoints = vec![SendEndpoint::InterThread(tx)];
        let mut ws: WriteStream<usize> =
            WriteStream::from_endpoints(endpoints, StreamId::new_deterministic());
        thread::spawn(move || {
            let w1 = Message::Watermark(Timestamp::new(vec![1]));
            let w2 = Message::Watermark(Timestamp::new(vec![2]));
            ws.send(w1).unwrap();
            ws.send(w2).unwrap();
        });
        let w1 = rt.block_on(rx.recv()).unwrap();
        match &*w1 {
            Message::Watermark(t) => {
                assert_eq!(t.time[0], 1);
            }
            _ => {
                panic!("Unexpected first watermark");
            }
        }
        let w2 = rt.block_on(rx.recv()).unwrap();
        match &*w2 {
            Message::Watermark(t) => {
                assert_eq!(t.time[0], 2);
            }
            _ => {
                panic!("Unexpected second watermark");
            }
        }
    }

    // Test that sends watermarks out of order. It expects that an error is raised.
    #[test]
    fn test_write_stream_out_of_order_watermark() -> Result<(), String> {
        let (tx, _rx) = mpsc::unbounded_channel();
        let endpoints = vec![SendEndpoint::InterThread(tx)];
        let mut ws: WriteStream<usize> =
            WriteStream::from_endpoints(endpoints, StreamId::new_deterministic());
        let w1 = Message::Watermark(Timestamp::new(vec![2]));
        ws.send(w1).unwrap();
        let w2 = Message::Watermark(Timestamp::new(vec![1]));
        match ws.send(w2) {
            Err(_) => Ok(()),
            _ => Err(String::from(
                "Didn't raise error when out-of-order were sent",
            )),
        }
    }

    // Test that sends a message with a timestamp lower than the low watermark. It expects that
    // an error is raised.
    #[test]
    fn test_write_stream_invalid_send() -> Result<(), String> {
        let (tx, _rx) = mpsc::unbounded_channel();
        let endpoints = vec![SendEndpoint::InterThread(tx)];
        let mut ws: WriteStream<usize> =
            WriteStream::from_endpoints(endpoints, StreamId::new_deterministic());
        let w1 = Message::Watermark(Timestamp::new(vec![2]));
        ws.send(w1).unwrap();
        let msg = Message::TimestampedData(TimestampedData::new(Timestamp::new(vec![1]), 2));
        match ws.send(msg) {
            Err(_) => Ok(()),
            _ => Err(String::from(
                "Didn't raise error when message with timestamp lower than low watermark was sent",
            )),
        }
    }
}
