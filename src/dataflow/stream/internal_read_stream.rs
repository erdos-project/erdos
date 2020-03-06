use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::{
    communication::{RecvEndpoint, TryRecvError},
    dataflow::{Data, Message, State, Timestamp},
    node::operator_event::OperatorEvent,
};

use super::{
    errors::{ReadError, TryReadError},
    EventMakerT, InternalStatefulReadStream, StreamId,
};

// TODO: split between system read streams and user accessible read streams to avoid Rc<RefCell<...>> in operator
pub struct InternalReadStream<D: Data> {
    /// The id of the stream.
    id: StreamId,
    /// User-defined stream name.
    name: String,
    /// Whether the stream is closed.
    closed: bool,
    /// The endpoint on which the stream receives data.
    recv_endpoint: Option<RecvEndpoint<Message<D>>>,
    /// Vector of stream bundles that must be invoked when this stream receives a message.
    children: Vec<Rc<RefCell<dyn EventMakerT<EventDataType = D>>>>,
    /// A vector on callbacks registered on the stream.
    callbacks: Vec<Arc<dyn Fn(Timestamp, D)>>,
    /// A vector of watermark callbacks registered on the stream.
    watermark_cbs: Vec<Arc<dyn Fn(&Timestamp)>>,
}

impl<D: Data> InternalReadStream<D> {
    /// Create a stream into which we can write data.
    pub fn new() -> Self {
        let id = StreamId::new_deterministic();
        Self {
            id,
            name: id.to_string(),
            closed: false,
            recv_endpoint: None,
            children: Vec::new(),
            callbacks: Vec::new(),
            watermark_cbs: Vec::new(),
        }
    }

    pub fn new_with_id_name(id: StreamId, name: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
            closed: false,
            recv_endpoint: None,
            children: Vec::new(),
            callbacks: Vec::new(),
            watermark_cbs: Vec::new(),
        }
    }

    pub fn get_id(&self) -> StreamId {
        self.id
    }

    pub fn get_name(&self) -> &str {
        &self.name[..]
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn from_endpoint(recv_endpoint: RecvEndpoint<Message<D>>, id: StreamId) -> Self {
        Self {
            id: id,
            name: id.to_string(),
            closed: false,
            recv_endpoint: Some(recv_endpoint),
            children: Vec::new(),
            callbacks: Vec::new(),
            watermark_cbs: Vec::new(),
        }
    }

    /// Add a callback to be invoked when the stream receives a message.
    pub fn add_callback<F: 'static + Fn(Timestamp, D)>(&mut self, callback: F) {
        self.callbacks.push(Arc::new(callback));
    }

    /// Add a callback to be invoked after the stream received, and the operator
    /// processed all the messages with a timestamp.
    pub fn add_watermark_callback<F: 'static + Fn(&Timestamp)>(&mut self, callback: F) {
        self.watermark_cbs.push(Arc::new(callback));
    }

    /// Returns a new instance of the stream with state associated to it.
    pub fn add_state<S: State>(
        &mut self,
        state: S,
    ) -> Rc<RefCell<InternalStatefulReadStream<D, S>>> {
        let child = Rc::new(RefCell::new(InternalStatefulReadStream::new(self, state)));
        self.children
            .push(Rc::clone(&child) as Rc<RefCell<dyn EventMakerT<EventDataType = D>>>);
        child
    }

    pub fn take_endpoint(&mut self) -> Option<RecvEndpoint<Message<D>>> {
        self.recv_endpoint.take()
    }

    /// Tries to read a message from a channel.
    ///
    /// Returns an immutable reference, or `None` if no messages are
    /// available at the moment (i.e., non-blocking read).
    pub fn try_read(&mut self) -> Result<Message<D>, TryReadError> {
        if self.closed {
            return Err(TryReadError::Closed);
        }
        let result = self
            .recv_endpoint
            .as_mut()
            .map_or(Err(TryReadError::Disconnected), |rx| {
                rx.try_read().map_err(TryReadError::from)
            });
        if result
            .as_ref()
            .map(Message::is_top_watermark)
            .unwrap_or(false)
        {
            self.closed = true;
            self.recv_endpoint = None;
        }
        result
    }

    /// Blocking read which polls the tokio channel.
    // TODO: make async or find a way to run on tokio.
    pub fn read(&mut self) -> Result<Message<D>, ReadError> {
        if self.closed {
            return Err(ReadError::Closed);
        }
        // Poll for the next message
        let result = self
            .recv_endpoint
            .as_mut()
            .map_or(Err(ReadError::Disconnected), |rx| loop {
                match rx.try_read() {
                    Ok(msg) => {
                        break Ok(msg);
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
            self.closed = true;
            self.recv_endpoint = None;
        }
        result
    }
}

impl<D: Data> Default for InternalReadStream<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: Data> EventMakerT for InternalReadStream<D> {
    type EventDataType = D;

    fn get_id(&self) -> StreamId {
        self.id
    }

    fn make_events(&self, msg: Message<Self::EventDataType>) -> Vec<OperatorEvent> {
        let mut events: Vec<OperatorEvent> = Vec::new();
        let mut child_events: Vec<OperatorEvent> = Vec::new();
        for child in self.children.iter() {
            child_events.append(&mut child.borrow_mut().make_events(msg.clone()));
        }
        match msg {
            Message::TimestampedData(msg) => {
                // Stateless callbacks may run in parallel, so create 1 event for each
                let stateless_cbs = self.callbacks.clone();
                for callback in stateless_cbs {
                    let msg_copy = msg.clone();
                    events.push(OperatorEvent::new(
                        msg.timestamp.clone(),
                        false,
                        move || {
                            (callback)(msg_copy.timestamp, msg_copy.data);
                        },
                    ))
                }
                // Add child events at the end
                events.append(&mut child_events);
            }
            Message::Watermark(timestamp) => {
                // Watermark callbacks must run in deterministic sequential order, so create 1 event for all
                let mut cbs: Vec<Box<dyn FnOnce()>> = Vec::new();
                let watermark_cbs = self.watermark_cbs.clone();
                for watermark_cb in watermark_cbs {
                    let cb = Arc::clone(&watermark_cb);
                    let timestamp_copy = timestamp.clone();
                    cbs.push(Box::new(move || (cb)(&timestamp_copy)))
                }
                for child_event in child_events {
                    cbs.push(child_event.callback);
                }
                if cbs.len() > 0 {
                    events.push(OperatorEvent::new(timestamp, true, move || {
                        for cb in cbs {
                            (cb)();
                        }
                    }))
                }
            }
        }
        events
    }
}
