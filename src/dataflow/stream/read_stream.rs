use std::{cell::RefCell, rc::Rc, sync::Arc};

use serde::Deserialize;

use crate::{
    communication::{RecvEndpoint, TryRecvError},
    dataflow::{Data, Message, State, Timestamp},
};

use super::{
    errors::{ReadError, TryReadError},
    IngestStream, LoopStream, StreamId, StreamT, WriteStream,
};

/// A [`ReadStream`] allows operators to read data from a corresponding [`WriteStream`].
///
/// An [`Operator`](crate::dataflow::operator::Operator) receives a [`ReadStream`] in its `new` and
/// `connect` functions. Operators can register both message and watermark callbacks on the
/// [`ReadStream`] to be invoked upon the receipt of a corresponding message. An operator can also
/// invoke the stream's [`read`](ReadStream::read) or [`try_read`](ReadStream::try_read) methods to
/// retrieve data from the streams in the [`run`](crate::dataflow::operator::Operator::run) method.
///
/// A driver receives a set of [`ReadStream`] s corresponding to the [`WriteStream`] s returned by
/// an operator's `connect` function. In order to connect operators, the driver can pass these
/// [`ReadStream`] s to other operators via the [`connect_x_write`](crate::connect_1_write)
/// family of macros. In order to read data from a [`ReadStream`], a driver can instantiate an
/// [`ExtractStream`](crate::dataflow::stream::ExtractStream) from the read stream.
///
/// # Examples
/// The following example shows an [`Operator`](crate::dataflow::operator::Operator) that takes in
/// a single [`ReadStream`] and prints the received value by requesting a callback.
/// ```
/// use erdos::dataflow::{Operator, ReadStream, Timestamp, OperatorConfig};
/// pub struct PrintOperator {}
///
/// impl PrintOperator {
///     pub fn new(config: OperatorConfig<()>, input_stream: ReadStream<u32>) -> Self {
///         // Request a callback upon receipt of every message.
///         input_stream.add_callback(
///             move |t: &Timestamp, msg: &u32| {
///                 Self::on_callback(t, msg)
///             },
///         );
///         Self {}
///     }
///
///     pub fn connect(input_stream: ReadStream<u32>) -> () {}
///
///     fn on_callback(t: &Timestamp, msg: &u32) {
///         println!("[{:?}] Received the value: {}", t, msg);
///     }
/// }
///
/// impl Operator for PrintOperator {}
/// ```
///
/// The following example shows an [`Operator`](crate::dataflow::operator::Operator) that takes in
/// a single [`ReadStream`] and prints the received value by querying for a value on the stream.
/// ```
/// use erdos::dataflow::{Operator, ReadStream, Timestamp, OperatorConfig};
/// pub struct SumOperator {
///     read_stream: ReadStream<u32>,
/// }
///
/// impl SumOperator {
///     pub fn new(config: OperatorConfig<()>, input_stream: ReadStream<u32>) -> Self {
///         Self {
///             read_stream: input_stream,
///         }
///     }
///
///     pub fn connect(input_stream: ReadStream<u32>) -> () {}
/// }
///
/// impl Operator for SumOperator {
///     fn run(&mut self) {
///         // Read 10 messages and print them.
///         for i in 1..10 {
///            let msg = self.read_stream.read().unwrap(); // blocking.
///            println!("[{:?}] Received the value: {}", msg.timestamp(), msg.data().unwrap());
///         }
///     }
/// }
/// ```
///
/// The examples in [`ExtractStream`](crate::dataflow::stream::ExtractStream) show how to use a
/// [`ReadStream`] returned by an [`Operator`](crate::dataflow::operator::Operator) to read data in
/// the driver.
pub struct ReadStream<D: Data> {
    /// The id of the stream.
    id: StreamId,
    /// User-defined stream name.
    name: String,
    /// Whether the stream is closed.
    is_closed: bool,
    /// The endpoint on which the stream receives data.
    recv_endpoint: Option<RecvEndpoint<Arc<Message<D>>>>,
}

impl<D: Data> ReadStream<D> {
    pub(crate) fn new(
        id: StreamId,
        name: &str,
        recv_endpoint: Option<RecvEndpoint<Arc<Message<D>>>>,
    ) -> Self {
        Self {
            id,
            name: name.to_string(),
            is_closed: false,
            recv_endpoint,
        }
    }

    /// Returns `true` if a top watermark message was sent or the [`ReadStream`] failed to set up.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Non-blocking read from the [`ReadStream`].
    ///
    /// Returns the Message available on the [`ReadStream`], or an [`Empty`](TryReadError::Empty)
    /// if no message is available.
    pub fn try_read(&mut self) -> Result<Message<D>, TryReadError> {
        if self.is_closed {
            return Err(TryReadError::Closed);
        }
        let result = self
            .recv_endpoint
            .as_mut()
            .map_or(Err(TryReadError::Disconnected), |rx| {
                rx.try_read()
                    .map(|msg| Message::clone(&msg))
                    .map_err(TryReadError::from)
            });
        if result
            .as_ref()
            .map(Message::is_top_watermark)
            .unwrap_or(false)
        {
            self.is_closed = true;
            self.recv_endpoint = None;
        }
        result
    }

    /// Blocking read from the [`ReadStream`].
    ///
    /// Returns the Message available on the [`ReadStream`].
    pub fn read(&mut self) -> Result<Message<D>, ReadError> {
        if self.is_closed {
            return Err(ReadError::Closed);
        }
        // Poll for the next message
        // TODO: call async_read and use some kind of runtime.
        let result = self
            .recv_endpoint
            .as_mut()
            .map_or(Err(ReadError::Disconnected), |rx| loop {
                match rx.try_read() {
                    Ok(msg) => {
                        break Ok(Message::clone(&msg));
                    }
                    Err(TryRecvError::Empty) => (),
                    Err(TryRecvError::Disconnected) => {
                        break Err(ReadError::Disconnected);
                    }
                    Err(TryRecvError::BincodeError(_)) => {
                        break Err(ReadError::SerializationError);
                    }
                }
            });
        if result
            .as_ref()
            .map(Message::is_top_watermark)
            .unwrap_or(false)
        {
            self.is_closed = true;
            self.recv_endpoint = None;
        }
        result
    }

    pub(crate) async fn async_read(&mut self) -> Result<Arc<Message<D>>, ReadError> {
        if self.is_closed {
            return Err(ReadError::Closed);
        }
        // Poll for the next message
        match self.recv_endpoint.as_mut() {
            Some(endpoint) => match endpoint.read().await {
                Ok(msg) => Ok(msg),
                // TODO: better error handling.
                _ => Err(ReadError::Disconnected),
            },
            None => Err(ReadError::Disconnected),
        }
    }
}

impl<D: Data> StreamT<D> for ReadStream<D> {
    /// Get the ID given to the stream by the constructor.
    fn id(&self) -> StreamId {
        self.id
    }

    /// Get the name of the stream.
    /// Returns a [`String`] version of the ID if the stream was not constructed with
    /// [`new_with_name`](ReadStream::new_with_name).
    fn name(&self) -> &str {
        &self.name
    }
}

unsafe impl<T: Data> Send for ReadStream<T> {}
unsafe impl<T: Data> Sync for ReadStream<T> {}
