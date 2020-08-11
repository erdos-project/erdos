/// Makes a callback which automatically flows watermarks to downstream
/// operators.
///
/// Note: this is intended as an internal macro invoked by
/// [`make_operator_executor`].
#[doc(hidden)]
#[macro_export]
macro_rules! flow_watermarks {
    (($($rs:ident),+), ($($ws:ident),+)) => {
        let cb_builder = $crate::make_callback_builder!(($($rs.add_state(())),+), ($($ws),+));
        cb_builder.borrow_mut().add_watermark_callback_with_priority(|timestamp, $($rs),+, $($ws),+| {
            $(
                match $ws.send(Message::new_watermark(timestamp.clone())) {
                    Ok(_) => (),
                    Err(_) => eprintln!("Error flowing watermark"),
                }
            )+
        }, 127);
    };
    // Cases in which the system doesn't need to flow watermarks
    (($($rs:ident),+), ()) => ();
    ((), ($($ws:ident),+)) => ();
    ((), ()) => ();
}

/// Calls `Operator::new(config, rs1, rs2, ..., ws1, ws2, ...)`
/// and returns the operator instance.
///
/// Note: this is an internal macro called by [`make_operator_executor`].
#[doc(hidden)]
#[macro_export]
macro_rules! make_operator {
    ($t:ty, $config:expr, ($($rs:ident),+), ($($ws:ident),*)) => {
        <$t>::new($config.clone(), $($rs.clone()),+, $($ws.clone()),*)
    };

    ($t:ty, $config:expr, (), ($($ws:ident),+)) => {
        <$t>::new($config.clone(), $($ws.clone()),*)
    };

    ($t:ty, $config:expr, (), ()) => {
        <$t>::new($config.clone())
    };
}

/// Makes a closure that initializes the operator and returns a corresponding
/// [`OperatorExecutor`](crate::node::operator_executor::OperatorExecutor).
///
/// Note: this is intended as an internal macro called by `connect_x_write`.
#[doc(hidden)]
#[macro_export]
macro_rules! make_operator_executor {
    ($t:ty, $config:expr, ($($rs:ident),*), ($($ws:ident),*)) => {{
        // Copy IDs to avoid moving streams into closure
        // Before: $rs is an identifier pointing to a read stream
        // $ws is an identifier pointing to a write stream
        $(
            let $rs = ($rs.get_id());
        )*
        $(
            let $ws = ($ws.get_id());
        )*
        // After: $rs is an identifier pointing to a read stream's StreamId
        // $ws is an identifier pointing to a write stream's StreamId
        move |channel_manager: Arc<Mutex<ChannelManager>>, control_sender: UnboundedSender<ControlMessage>, mut control_receiver: UnboundedReceiver<ControlMessage>| {
            let mut op_ex_streams: Vec<Box<dyn OperatorExecutorStreamT>> = Vec::new();
            // Before: $rs is an identifier pointing to a read stream's StreamId
            // $ws is an identifier pointing to a write stream's StreamId
            $(
                let $rs = {
                    let recv_endpoint = channel_manager.lock().unwrap().take_recv_endpoint($rs).unwrap();
                    let read_stream = ReadStream::from(InternalReadStream::from_endpoint(recv_endpoint, $rs));
                    op_ex_streams.push(
                        Box::new(OperatorExecutorStream::from(&read_stream))
                    );
                    read_stream
                };
            )*
            $(
                let $ws = {
                    let send_endpoints = channel_manager.lock().unwrap().get_send_endpoints($ws).unwrap();
                    WriteStream::from_endpoints(send_endpoints, $ws)
                };
            )*
            // After: $rs is an identifier pointing to ReadStream
            // $ws is an identifier pointing to WriteStream
            let mut config = $config.clone();
            config.node_id = channel_manager.lock().unwrap().node_id();
            let flow_watermarks = config.flow_watermarks;
            // TODO: set operator name?
            let mut op = $crate::make_operator!($t, config.clone(), ($($rs),*), ($($ws),*));
            // Pass on watermarks
            if flow_watermarks {
                $crate::flow_watermarks!(($($rs),*), ($($ws),*));
            }
            // Notify node that operator is done setting up
            if let Err(e) = control_sender.send(ControlMessage::OperatorInitialized(config.id)) {
                panic!("Error sending OperatorInitialized message to control handler: {:?}", e);
            }
            let mut op_executor = OperatorExecutor::new(op, config, op_ex_streams, control_receiver);
            op_executor
        }
    }};
}

/// Imports crates needed to run [`register`].
///
/// Note: this is intended as an internal macro called by [`register`].
#[doc(hidden)]
#[macro_export]
macro_rules! imports {
    () => {
        use std::{
            cell::RefCell,
            rc::Rc,
            sync::{Arc, Mutex},
            thread,
            time::Duration,
        };
        use $crate::slog;
        use $crate::tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
        use $crate::{
            self,
            communication::ControlMessage,
            dataflow::graph::default_graph,
            dataflow::stream::{InternalReadStream, WriteStreamT},
            dataflow::{Message, Operator, ReadStream, WriteStream},
            node::operator_executor::{
                OperatorExecutor, OperatorExecutorStream, OperatorExecutorStreamT,
            },
            scheduler::channel_manager::ChannelManager,
            OperatorId,
        };
    };
}

/// Registers and operator and streams produced by that operator to the
/// dataflow graph and the stream manager.
///
/// Note: this is intended as an internal macro called by connect_x_write!
#[doc(hidden)]
#[macro_export]
macro_rules! register {
    ($t:ty, $config:expr, ($($rs:ident),*), ($($ws:ident),*)) => {{
        // Import necesary structs, modules, and functions.
        $crate::imports!();

        let mut config = $config.clone();
        config.id = OperatorId::new_deterministic();
        let config_copy = config.clone();

        // No-op that throws compile-time error if types in `new` and `connect` don't match.
        if false {
            let mut op = $crate::make_operator!($t, config.clone(), ($($rs),*), ($($ws),*));
            Operator::run(&mut op)
        }

        // Add operator to dataflow graph.
        let read_stream_ids = vec![$($rs.get_id()),*];
        let write_stream_ids = vec![$($ws.get_id()),*];
        let op_runner = $crate::make_operator_executor!($t, config_copy, ($($rs),*), ($($ws),*));
        default_graph::add_operator(config.id, config.name.clone(), config.node_id, read_stream_ids, write_stream_ids, op_runner);
        $(
            default_graph::add_operator_stream(config.id, &$ws);
        )*
        // Register streams with stream manager.
        ($(ReadStream::from(&$ws)),*)
    }};
}

/// Connects read streams to an operator that writes on 0 streams.
///
/// Use:
/// ```ignore
/// connect_0_write!(MyOp, arg, read_stream_1, read_stream_2, ...);
/// ```
#[macro_export]
macro_rules! connect_0_write {
    ($t:ty, $config:expr $(,$s:ident)*) => {{
        // Cast streams to read streams to avoid type errors.
        $(
            let $s = (&$s).into();
        )*
        <$t>::connect($(&$s),*);
        $crate::register!($t, $config, ($($s),*), ())
    }};
}

/// Connects read streams to an operator that writes on 1 stream.
///
/// Use:
/// ```ignore
/// let read_stream_3 = connect_3_write!(MyOp, arg, read_stream_1, read_stream_2, ...);
/// ```
#[macro_export]
macro_rules! connect_1_write {
    ($t:ty, $config:expr $(,$s:ident)*) => {{
        // Cast streams to read streams to avoid type errors.
        $(
            let $s = (&$s).into();
        )*
        let ws = <$t>::connect($(&$s),*);
        $crate::register!($t, $config, ($($s),*), (ws))
    }};
}

/// Connects read streams to an operator that writes on 2 streams.
///
/// Use:
/// ```ignore
/// let (read_stream_3, read_stream_4) = connect_3_write!(MyOp, arg, read_stream_1, read_stream_2, ...);
/// ```
#[macro_export]
macro_rules! connect_2_write {
    ($t:ty, $config:expr $(,$s:ident)*) => {{
        // Cast streams to read streams to avoid type errors.
        $(
            let $s = (&$s).into();
        )*
        let (ws1, ws2) = <$t>::connect($(&$s),*);
        $crate::register!($t, $config, ($($s),*), (ws1, ws2))
    }};
}

/// Connects read streams to an operator that writes on 3 streams.
///
/// Use:
/// ```ignore
/// let (read_stream_3, read_stream_4, read_stream_5) = connect_3_write!(MyOp, arg, read_stream_1, read_stream_2, ...);
/// ```
#[macro_export]
macro_rules! connect_3_write {
    ($t:ty, $config:expr $(,$s:ident)*) => {{
        // Cast streams to read streams to avoid type errors.
        $(
            let $s = (&$s).into();
        )*
        let (ws1, ws2, ws3) = <$t>::connect($(&$s),*);
        $crate::register!($t, $config, ($($s),*), (ws1, ws2, ws3))
    }};
}

/// Makes a callback builder that can register watermark callbacks across multiple streams.
///
/// Note: an internal macro invoked by `add_watermark_callback`.
#[doc(hidden)]
#[macro_export]
macro_rules! make_callback_builder {
    // Base case: 1 read stream, 0 write streams, state
    (($rs_head:expr), (), $state:expr) => {{
        use std::{cell::RefCell, rc::Rc};
        Rc::new(RefCell::new($rs_head.add_state($state)))
    }};

    // Base case: 1 read stream
    (($rs_head:expr), ($($ws:expr),*)) => {{
        use std::{cell::RefCell, rc::Rc};
        use $crate::dataflow::callback_builder::MultiStreamEventMaker;


        let cb_builder = Rc::new(RefCell::new($rs_head));
        $(
            let cb_builder = cb_builder.borrow_mut().add_write_stream(&$ws);
        )*
        cb_builder
    }};

    // Entry point: multiple read streams, state
    (($($rs:expr),+), ($($ws:expr),*), $state:expr) => {{
        use $crate::dataflow::callback_builder::MultiStreamEventMaker;

        make_callback_builder!(($($rs),+), ($($ws),*)).borrow_mut().add_state($state)
    }};

    // Recursive call: multiple read streams
    (($rs_head:expr, $($rs:expr),*), ($($ws:expr),*)) => {{
        use std::{cell::RefCell, rc::Rc};

        let cb_builder = Rc::new(RefCell::new($rs_head));
        $(
            let cb_builder = cb_builder.borrow_mut().add_read_stream(&$rs);
        )*
        $(
            let cb_builder = cb_builder.borrow_mut().add_write_stream(&$ws);
        )*
        cb_builder
    }};
}

/// Adds a watermark callback across several read streams.
///
/// Watermark callbacks are invoked in deterministic order.
/// Optionally add a state that is shared across callbacks.
///
/// Use:
/// ```ignore
/// add_watermark_callback!((read_stream_1, read_stream_2, ...),
///                        (write_stream_1, write_stream_2, ...)
///                        (callback_1, callback_2, ...), state?);
/// ```
#[macro_export]
macro_rules! add_watermark_callback {
    (($($rs:expr),+), ($($ws:expr),*), ($($cb:expr),+), $state:expr) => (
        let cb_builder = $crate::make_callback_builder!(($($rs),+), ($($ws),*), $state);
        $(
            cb_builder.borrow_mut().add_watermark_callback($cb);
        )+
    );
    (($($rs:expr),+), ($($ws:expr),*), ($($cb:expr),+)) => (
        let cb_builder = $crate::make_callback_builder!(($($rs),+), ($($ws),*));
        $(
            cb_builder.borrow_mut().add_watermark_callback($cb);
        )+
    );
}
