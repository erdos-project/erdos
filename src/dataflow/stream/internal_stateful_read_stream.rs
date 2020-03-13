use std::sync::Arc;
use std::{cell::RefCell, rc::Rc};

use crate::{
    dataflow::{
        callback_builder::MultiStreamEventMaker,
        state::{AccessContext, ManagedState},
        Data, Message, State, Timestamp,
    },
    node::operator_event::OperatorEvent,
};

use super::{EventMakerT, InternalReadStream, StreamId};

/// Stream that has associated some state with it.
pub struct InternalStatefulReadStream<D: Data, S: State> {
    /// StreamId of the stream.
    id: StreamId,
    /// State associated with the stream. The state is not wrapped with a mutex because the
    /// ```OperatorExecutor``` ensures that no two callbacks that change the state execute
    /// simultaneously.
    state: Arc<S>,
    /// Callbacks registered on the stream.
    callbacks: Vec<Arc<dyn Fn(Timestamp, D, &mut S)>>,
    /// Watermark callbacks registered on the stream.
    watermark_cbs: Vec<Arc<dyn Fn(&Timestamp, &mut S)>>,
    /// Vector of stream bundles that must be invoked when this stream receives a message.
    children: RefCell<Vec<Rc<RefCell<dyn MultiStreamEventMaker>>>>,
}

impl<D: Data, S: State> InternalStatefulReadStream<D, S> {
    pub fn new(stream: &mut InternalReadStream<D>, state: S) -> Self {
        Self {
            id: stream.get_id(),
            state: Arc::new(state),
            callbacks: Vec::new(),
            watermark_cbs: Vec::new(),
            children: RefCell::new(Vec::new()),
        }
    }

    pub fn get_id(&self) -> StreamId {
        self.id
    }

    /// Add a callback to be invoked when the stream receives a message.
    /// The callback will be invoked for each message, and will receive the
    /// message and the stream's state as arguments.
    pub fn add_callback<F: 'static + Fn(Timestamp, D, &mut S)>(&mut self, callback: F) {
        self.callbacks.push(Arc::new(callback));
    }

    /// Add a callback to be invoked after the stream received, and the operator
    /// processed all the messages with a timestamp.
    pub fn add_watermark_callback<F: 'static + Fn(&Timestamp, &mut S)>(&mut self, callback: F) {
        self.watermark_cbs.push(Arc::new(callback));
    }

    /// Gets a reference to the stream state.
    pub fn get_state(&self) -> Arc<S> {
        Arc::clone(&self.state)
    }

    pub fn add_child<T: 'static + MultiStreamEventMaker>(&self, child: Rc<RefCell<T>>) {
        self.children.borrow_mut().push(child);
    }
}

impl<D: Data, S: State> EventMakerT for InternalStatefulReadStream<D, S> {
    type EventDataType = D;

    fn get_id(&self) -> StreamId {
        self.id
    }

    fn make_events(&self, msg: Message<Self::EventDataType>) -> Vec<OperatorEvent> {
        let mut events: Vec<OperatorEvent> = Vec::new();
        match msg {
            Message::TimestampedData(msg) => {
                // Stateful callbacks may not run in parallel because they access shared state,
                // so create 1 callback for all
                let stateful_cbs = self.callbacks.clone();
                let mut state_arc = Arc::clone(&self.state);
                if !stateful_cbs.is_empty() {
                    events.push(OperatorEvent::new(
                        msg.timestamp.clone(),
                        false,
                        move || {
                            for callback in stateful_cbs {
                                let msg_copy = msg.clone();
                                let state_ref_mut =
                                    unsafe { Arc::get_mut_unchecked(&mut state_arc) };
                                state_ref_mut.set_access_context(AccessContext::Callback);
                                state_ref_mut.set_current_time(msg.timestamp.clone());
                                (callback)(msg_copy.timestamp, msg_copy.data, state_ref_mut)
                            }
                        },
                    ));
                }
            }
            Message::Watermark(timestamp) => {
                // Watermark callback
                let mut cbs: Vec<Box<dyn FnOnce()>> = Vec::new();
                let watermark_cbs = self.watermark_cbs.clone();
                for watermark_cb in watermark_cbs {
                    let cb = Arc::clone(&watermark_cb);
                    let timestamp_copy = timestamp.clone();
                    let mut state_arc = Arc::clone(&self.state);
                    cbs.push(Box::new(move || {
                        let state_ref_mut = unsafe { Arc::get_mut_unchecked(&mut state_arc) };
                        state_ref_mut.set_access_context(AccessContext::WatermarkCallback);
                        state_ref_mut.set_current_time(timestamp_copy.clone());
                        (cb)(&timestamp_copy, state_ref_mut)
                    }))
                }
                // Notify children of watermark and get events
                for child in self.children.borrow().iter() {
                    let mut child = child.borrow_mut();
                    for child_event in child.receive_watermark(self.id, timestamp.clone()) {
                        cbs.push(child_event.callback);
                    }
                }
                if !cbs.is_empty() {
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
