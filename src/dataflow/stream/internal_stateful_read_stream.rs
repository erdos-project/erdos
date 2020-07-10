use std::{cell::RefCell, collections::HashSet, rc::Rc, sync::Arc};

use crate::{
    dataflow::{
        callback_builder::MultiStreamEventMaker,
        state::{AccessContext, ManagedState},
        Data, Message, State, Timestamp,
    },
    node::operator_event::OperatorEvent,
    Uuid,
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
    state_id: Uuid,
    /// Callbacks registered on the stream.
    callbacks: Vec<Arc<dyn Fn(&Timestamp, &D, &mut S)>>,
    /// Watermark callbacks registered on the stream.
    watermark_cbs: Vec<(Arc<dyn Fn(&Timestamp, &mut S)>, i8)>,
    /// Vector of stream bundles that must be invoked when this stream receives a message.
    children: RefCell<Vec<Rc<RefCell<dyn MultiStreamEventMaker>>>>,
}

impl<D: Data, S: State> InternalStatefulReadStream<D, S> {
    pub fn new(stream: &mut InternalReadStream<D>, state: S) -> Self {
        Self {
            id: stream.get_id(),
            state: Arc::new(state),
            state_id: Uuid::new_deterministic(),
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
    pub fn add_callback<F: 'static + Fn(&Timestamp, &D, &mut S)>(&mut self, callback: F) {
        self.callbacks.push(Arc::new(callback));
    }

    /// Add a callback to be invoked after the stream received, and the operator
    /// processed all the messages with a timestamp.
    pub fn add_watermark_callback<F: 'static + Fn(&Timestamp, &mut S)>(&mut self, callback: F) {
        self.add_watermark_callback_with_priority(callback, 0);
    }

    /// Add a callback to be invoked after the stream received, and the operator
    /// processed all the messages with a timestamp.
    pub(crate) fn add_watermark_callback_with_priority<F: 'static + Fn(&Timestamp, &mut S)>(
        &mut self,
        callback: F,
        priority: i8,
    ) {
        self.watermark_cbs.push((Arc::new(callback), priority));
    }

    /// Gets a reference to the stream state.
    pub fn get_state(&self) -> Arc<S> {
        Arc::clone(&self.state)
    }

    pub fn get_state_id(&self) -> Uuid {
        self.state_id
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

    fn make_events(&self, msg: Arc<Message<Self::EventDataType>>) -> Vec<OperatorEvent> {
        let mut events: Vec<OperatorEvent> = Vec::new();
        let mut write_ids = HashSet::with_capacity(1);
        write_ids.insert(self.state_id);

        match msg.as_ref() {
            Message::TimestampedData(_) => {
                // Stateful callbacks may not run in parallel because they access shared state,
                // so create 1 callback for all
                let stateful_cbs = self.callbacks.clone();
                for callback in stateful_cbs {
                    // TODO: replace with RW lock or time-versioned data structure to prevent conflicts.
                    let msg_arc = Arc::clone(&msg);
                    let mut state_arc = Arc::clone(&self.state);
                    events.push(OperatorEvent::new(
                        msg.timestamp().clone(),
                        false,
                        0,
                        HashSet::with_capacity(0),
                        write_ids.clone(),
                        move || {
                            let state_ref_mut = unsafe { Arc::get_mut_unchecked(&mut state_arc) };
                            state_ref_mut.set_access_context(AccessContext::Callback);
                            state_ref_mut.set_current_time(msg_arc.timestamp().clone());
                            (callback)(msg_arc.timestamp(), msg_arc.data().unwrap(), state_ref_mut)
                        },
                    ));
                }
            }
            Message::Watermark(timestamp) => {
                // Watermark callback
                let watermark_cbs = self.watermark_cbs.clone();
                for (watermark_cb, priority) in watermark_cbs {
                    let cb = Arc::clone(&watermark_cb);
                    let timestamp_copy = timestamp.clone();
                    let mut state_arc = Arc::clone(&self.state);
                    events.push(OperatorEvent::new(
                        timestamp_copy.clone(),
                        true,
                        priority,
                        HashSet::with_capacity(0),
                        write_ids.clone(),
                        move || {
                            let state_ref_mut = unsafe { Arc::get_mut_unchecked(&mut state_arc) };
                            state_ref_mut.set_access_context(AccessContext::WatermarkCallback);
                            state_ref_mut.set_current_time(timestamp_copy.clone());
                            (cb)(&timestamp_copy, state_ref_mut)
                        },
                    ));
                }
                // Notify children of watermark and get events
                for child in self.children.borrow().iter() {
                    let mut child = child.borrow_mut();
                    events.extend(child.receive_watermark(self.id, timestamp.clone()));
                }
            }
        }
        events
    }
}
