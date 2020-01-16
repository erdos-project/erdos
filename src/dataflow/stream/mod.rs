use crate::{
    dataflow::{Data, Message},
    node::operator_event::OperatorEvent,
};

mod errors;

pub mod extract_stream;
pub mod ingest_stream;
pub mod internal_read_stream;
pub mod internal_stateful_read_stream;
pub mod loop_stream;
pub mod read_stream;
pub mod stateful_read_stream;
pub mod write_stream;

pub use errors::WriteStreamError;
pub use extract_stream::ExtractStream;
pub use ingest_stream::IngestStream;
pub use internal_read_stream::InternalReadStream;
pub use internal_stateful_read_stream::InternalStatefulReadStream;
pub use loop_stream::LoopStream;
pub use read_stream::ReadStream;
pub use stateful_read_stream::StatefulReadStream;
pub use write_stream::WriteStream;

pub type StreamId = crate::Uuid;

pub trait EventMakerT {
    type EventDataType: Data;

    /// Returns the id of the stream.
    fn get_id(&self) -> StreamId;

    /// Returns the vector of events that a message receipt generates.
    fn make_events(&self, msg: Message<Self::EventDataType>) -> Vec<OperatorEvent>;
}

pub trait ReadStreamT: EventMakerT {
    type DataType: Data;

    /// Returns the id of the stream.
    fn get_id(&self) -> StreamId;

    /// Tries to read a message from a channel.
    ///
    /// Returns an immutable reference, or `None` if no messages are
    /// available at the moment (i.e., non-blocking read).
    fn try_read(&mut self) -> Option<Message<Self::DataType>>;

    /// Blocking read. Returns `None` if the stream doesn't have a receive endpoint.
    fn read(&mut self) -> Option<Message<Self::DataType>>;
}

pub trait WriteStreamT<D: Data> {
    /// Sends a messsage to a channel.
    fn send(&mut self, msg: Message<D>) -> Result<(), WriteStreamError>;
}

#[cfg(test)]
mod tests {
    use super::{WriteStream, WriteStreamT};
    use crate::communication::SendEndpoint;
    use crate::dataflow::{message::TimestampedData, stream::StreamId, Message, Timestamp};
    use std::sync::mpsc;
    use std::thread;

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
        let ws: WriteStream<usize> = WriteStream::new();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ws = rx.recv().unwrap();
        });
        tx.send(ws).unwrap();
    }

    // Test that sends two messages on a write stream. It checks if both messages are received.
    #[test]
    fn test_write_stream_send() {
        let (tx, rx) = mpsc::channel();
        let endpoints = vec![SendEndpoint::InterThread(tx)];
        let mut ws: WriteStream<usize> =
            WriteStream::from_endpoints(endpoints, StreamId::new_deterministic());
        thread::spawn(move || {
            let msg1 = Message::TimestampedData(TimestampedData::new(Timestamp::new(vec![0]), 1));
            let msg2 = Message::TimestampedData(TimestampedData::new(Timestamp::new(vec![0]), 2));
            ws.send(msg1).unwrap();
            ws.send(msg2).unwrap();
        });
        let first_msg = rx.recv().unwrap();
        match first_msg {
            Message::TimestampedData(td) => {
                assert_eq!(td.data, 1);
            }
            _ => {
                panic!("Unexpected first message");
            }
        }
        let second_msg = rx.recv().unwrap();
        match second_msg {
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
        let (tx, rx) = mpsc::channel();
        let endpoints = vec![SendEndpoint::InterThread(tx)];
        let mut ws: WriteStream<usize> =
            WriteStream::from_endpoints(endpoints, StreamId::new_deterministic());
        thread::spawn(move || {
            let w1 = Message::Watermark(Timestamp::new(vec![1]));
            let w2 = Message::Watermark(Timestamp::new(vec![2]));
            ws.send(w1).unwrap();
            ws.send(w2).unwrap();
        });
        let w1 = rx.recv().unwrap();
        match w1 {
            Message::Watermark(t) => {
                assert_eq!(t.time[0], 1);
            }
            _ => {
                panic!("Unexpected first watermark");
            }
        }
        let w2 = rx.recv().unwrap();
        match w2 {
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
        let (tx, _rx) = mpsc::channel();
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
        let (tx, _rx) = mpsc::channel();
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
