use std::sync::Arc;

use crate::{
    communication::{RecvEndpoint, TryRecvError},
    dataflow::{Data, Message},
};

use super::{
    errors::{ReadError, TryReadError},
    StreamId,
};

/// A [`ReadStream`] allows operators to pull [`Message`]s from a [stream](crate::dataflow::stream).
///
/// # Example
/// The following example shows an operator that prints out messages received from a [`ReadStream`].
/// ```
/// # use std::marker::PhantomData;
/// # use erdos::dataflow::{operator::{OperatorConfig, Sink}, context::SinkContext, Data, ReadStream};
/// #
/// struct PrintMessageOperator<D: Data> {
///     phantom: PhantomData<D>,
/// }
///
/// impl<D: Data> Sink<(), D> for PrintMessageOperator<D> {
/// #    fn on_data(&mut self, ctx: &mut SinkContext<()>, data: &D) {}
/// #    fn on_watermark(&mut self, ctx: &mut SinkContext<()>) {}
/// #
///     fn run(&mut self, config: &OperatorConfig, read_stream: &mut ReadStream<D>) {
///         while let Ok(message) = read_stream.read() {
///             println!("Recieved message: {:?}", message);
///         }
///     }
/// }
/// ```
///
/// The examples in [`ExtractStream`](crate::dataflow::stream::ExtractStream) show how to
/// pull data from a stream in the driver.
pub struct ReadStream<D: Data> {
    /// The id of the stream.
    id: StreamId,
    /// The name of the stream.
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

        // TODO: Close the stream?
    }

    /// Get the ID given to the stream by the constructor.
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// Get the name of the stream.
    pub fn name(&self) -> String {
        self.name.clone()
    }
}

unsafe impl<T: Data> Send for ReadStream<T> {}
unsafe impl<T: Data> Sync for ReadStream<T> {}
