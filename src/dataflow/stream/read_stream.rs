use std::{cell::RefCell, rc::Rc};

use serde::Deserialize;

use crate::dataflow::{Data, Message, State, Timestamp};

use super::{
    errors::{ReadError, TryReadError},
    IngestStream, InternalReadStream, LoopStream, StreamId, WriteStream,
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
#[derive(Clone, Default)]
pub struct ReadStream<D: Data> {
    /// Stores information and internal information about the stream
    internal_stream: Rc<RefCell<InternalReadStream<D>>>,
}

impl<D: Data> ReadStream<D> {
    /// Returns a new instance of the [`ReadStream`].
    /// Note that ERDOS automatically converts the [`WriteStream`]s returned by an
    /// [`Operator`](crate::dataflow::operator::Operator) to a corresponding [`ReadStream`].
    pub fn new() -> Self {
        Self {
            internal_stream: Rc::new(RefCell::new(InternalReadStream::new())),
        }
    }

    /// Returns a new instance of the [`ReadStream`] with the given name..
    /// Note that ERDOS automatically converts the [`WriteStream`]s returned by an
    /// [`Operator`](crate::dataflow::operator::Operator) to a corresponding [`ReadStream`].
    ///
    /// # Arguments
    /// * name - The name of the stream.
    pub fn new_with_name(name: String) -> Self {
        Self {
            internal_stream: Rc::new(RefCell::new(InternalReadStream::new_with_id_name(
                StreamId::new_deterministic(),
                &name,
            ))),
        }
    }

    /// Request a callback on the receipt of a
    /// [`TimestampedData`](crate::dataflow::message::Message::TimestampedData) message on the
    /// stream.
    ///
    /// # Arguments
    /// * callback - The callback to be invoked when a message is received.
    pub fn add_callback<F: 'static + Fn(&Timestamp, &D)>(&self, callback: F) {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Registering a message callback on the ReadStream {} (ID: {})",
            self.get_name(),
            self.get_id()
        );
        self.internal_stream.borrow_mut().add_callback(callback);
    }

    /// Request a callback on the receipt of a
    /// [`Watermark`](crate::dataflow::message::Message::Watermark) message on the
    /// stream.
    ///
    /// # Arguments
    /// * callback - The callback to be invoked when a watermark is received.
    pub fn add_watermark_callback<F: 'static + Fn(&Timestamp)>(&self, callback: F) {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Registering a watermark callback on the ReadStream {} (ID: {})",
            self.get_name(),
            self.get_id()
        );
        self.internal_stream
            .borrow_mut()
            .add_watermark_callback(callback);
    }

    /// Get the ID given to the stream by the constructor.
    pub fn get_id(&self) -> StreamId {
        self.internal_stream.borrow().get_id()
    }

    /// Get the name of the stream.
    /// Returns a [`str`] version of the ID if the stream was not constructed with
    /// [`new_with_name`](ReadStream::new_with_name).
    pub fn get_name(&self) -> String {
        self.internal_stream.borrow().get_name().to_string()
    }

    /// Returns `true` if a top watermark message was sent or the [`ReadStream`] failed to set up.
    pub fn is_closed(&self) -> bool {
        self.internal_stream.borrow().is_closed()
    }

    /// Non-blocking read from the [`ReadStream`].
    ///
    /// Returns the Message available on the [`ReadStream`], or an [`Empty`](TryReadError::Empty)
    /// if no message is available.
    pub fn try_read(&self) -> Result<Message<D>, TryReadError> {
        self.internal_stream.borrow_mut().try_read()
    }

    /// Blocking read from the [`ReadStream`].
    ///
    /// Returns the Message available on the [`ReadStream`].
    pub fn read(&self) -> Result<Message<D>, ReadError> {
        self.internal_stream.borrow_mut().read()
    }
}

impl<D: Data> From<&ReadStream<D>> for ReadStream<D> {
    fn from(read_stream: &ReadStream<D>) -> Self {
        read_stream.clone()
    }
}

impl<D: Data> From<&WriteStream<D>> for ReadStream<D> {
    fn from(write_stream: &WriteStream<D>) -> Self {
        Self::from(InternalReadStream::new_with_id_name(
            write_stream.get_id(),
            write_stream.get_name(),
        ))
    }
}

impl<D> From<&LoopStream<D>> for ReadStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn from(loop_stream: &LoopStream<D>) -> Self {
        Self::from(InternalReadStream::new_with_id_name(
            loop_stream.get_id(),
            loop_stream.get_name(),
        ))
    }
}

impl<D> From<&IngestStream<D>> for ReadStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn from(ingest_stream: &IngestStream<D>) -> Self {
        Self::from(InternalReadStream::new_with_id_name(
            ingest_stream.get_id(),
            ingest_stream.get_name(),
        ))
    }
}

impl<D: Data> From<InternalReadStream<D>> for ReadStream<D> {
    fn from(internal_stream: InternalReadStream<D>) -> Self {
        Self {
            internal_stream: Rc::new(RefCell::new(internal_stream)),
        }
    }
}

impl<D: Data> From<Rc<RefCell<InternalReadStream<D>>>> for ReadStream<D> {
    fn from(internal_stream: Rc<RefCell<InternalReadStream<D>>>) -> Self {
        Self { internal_stream }
    }
}

impl<D: Data> From<&ReadStream<D>> for Rc<RefCell<InternalReadStream<D>>> {
    fn from(read_stream: &ReadStream<D>) -> Self {
        Rc::clone(&read_stream.internal_stream)
    }
}

unsafe impl<T: Data> Send for ReadStream<T> {}
