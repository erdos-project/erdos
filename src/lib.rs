//! # ERDOS
//!
//! `ERDOS` is a platform  for developing self-driving cars and robotics
//!  applications.
//!
//! `ERDOS` is a streaming dataflow system designed for self-driving car
//! pipelines and robotics applications.
//!
//! Components of the pipelines are implemented as **operators** which
//! are connected by **data streams**. The set of operators and streams
//! forms the **dataflow graph**, the representation of the pipline that
//! `ERDOS` processes.
//!
//! Applications define the dataflow graph by connecting operators to streams
//! in the **driver** section of the program. Operators are typically
//! implemented elsewhere.
//!
//! `ERDOS` is designed for low latency. Self-driving car pipelines require
//! end-to-end deadlines on the order of hundreds of milliseconds for safe
//! driving. Similarly, self-driving cars typically process gigabytes per
//! second of data on small clusters. Therefore, `ERDOS` is optimized to
//! send small amounts of data (gigabytes as opposed to terabytes)
//! as quickly as possible.
//!
//! `ERDOS` provides determinisim through **watermarks**. Low watermarks
//! are a bound on the age of messages received and operators will ignore
//! any messages older than the most recent watermark received. By processing
//! on watermarks, applications can avoid non-determinism from processing
//! messages out of order.

#![feature(get_mut_unchecked)]
#![feature(specialization)]
#![feature(box_into_pin)]

extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate bincode;
extern crate clap;
#[macro_use]
extern crate lazy_static;
#[macro_use]
pub extern crate slog;
extern crate slog_term;
pub extern crate tokio;

// Libraries used in this file.
use clap::{App, Arg};
use rand::{Rng, SeedableRng, StdRng};
use serde::{Deserialize, Serialize};
use slog::{Drain, Logger};
use slog_term::term_full;
use std::{cell::RefCell, fmt};
use uuid;

// Export the modules to be visible outside of the library.
pub mod communication;
pub mod configuration;
pub mod dataflow;
pub mod node;
#[cfg(feature = "python")]
pub mod python;
pub mod scheduler;

pub use configuration::Configuration;
pub use dataflow::OperatorConfig;

/// Makes a callback which automatically flows watermarks to downstream operators.
///
/// Note: this is intended as an internal macro invoked by `make_operator_executor!`
#[macro_export]
macro_rules! flow_watermarks {
    (($($rs:ident),+), ($($ws:ident),+)) => {
        $crate::add_watermark_callback!(($($rs.add_state(())),+), ($($ws),+), (|timestamp, $($rs),+, $($ws),+| {
            $(
                match $ws.send(Message::new_watermark(timestamp.clone())) {
                    Ok(_) => (),
                    Err(_) => eprintln!("Error flowing watermark"),
                }
            )+
        }));
    };
    // Cases in which the system doesn't need to flow watermarks
    (($($rs:ident),+), ()) => ();
    ((), ($($ws:ident),+)) => ();
    ((), ()) => ();
}

/// Makes a callback which automatically flows watermarks to downstream operators.
///
/// Note: this is intended as an internal macro invoked by `make_operator_executor!`
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

/// Makes a closure that initializes the operator and returns the corresponding [`OperatorExecutor`].
///
/// Note: this is intended as an internal macro called by connect_x_write!
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
            let logger = $crate::get_terminal_logger();
            // TODO: set operator name?
            let mut op = $crate::make_operator!($t, config.clone(), ($($rs),*), ($($ws),*));
            // Pass on watermarks
            if flow_watermarks {
                $crate::flow_watermarks!(($($rs),*), ($($ws),*));
            }
            // Notify node that operator is done setting up
            if let Err(e) = control_sender.send(ControlMessage::OperatorInitialized(config.id)) {
                slog::error!(
                    logger,
                    "Error sending OperatorInitialized message to control handler: {:?}", e
                );
            }
            let mut op_executor = OperatorExecutor::new(op, config, op_ex_streams, control_receiver, logger);
            op_executor
        }
    }};
}

/// Imports crates needed to run register!
///
/// Note: this is intended as an internal macro called by register!
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

/// Registers and operator and streams produced by that operator to the dataflow graph and the stream manager.
///
/// Note: this is intended as an internal macro called by connect_x_write!
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
/// connect_3_write!(MyOp, arg, read_stream_1, read_stream_2, ...);
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
/// let read_stream_3 = connect_3_write!(MyOp, arg, read_stream_1, read_stream_2, ...);
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
/// let (read_stream_3, read_stream_4) = connect_3_write!(MyOp, arg, read_stream_1, read_stream_2, ...);
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
/// let (read_stream_3, read_stream_4, read_stream_5) = connect_3_write!(MyOp, arg, read_stream_1, read_stream_2, ...);
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
/// add_watermark_callback!((read_stream_1, read_stream_2, ...),
///                        (write_stream_1, write_stream_2, ...)
///                        (callback_1, callback_2, ...), state?);
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

pub type OperatorId = Uuid;

// Random number generator which should be the same accross threads and processes.
thread_local!(static RNG: RefCell<StdRng>= RefCell::new(StdRng::from_seed(&[1913, 03, 26])));

/// Produces a deterministic, unique ID.
pub fn generate_id() -> Uuid {
    RNG.with(|rng| {
        let mut bytes = [0u8; 16];
        rng.borrow_mut().fill_bytes(&mut bytes);
        Uuid(bytes)
    })
}

/// Wrapper around uuid::Uuid that implements Abomonation for fast serialization.
#[derive(Abomonation, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Uuid(uuid::Bytes);

impl Uuid {
    pub fn new_v4() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }

    pub fn new_deterministic() -> Self {
        generate_id()
    }

    pub fn nil() -> Uuid {
        Uuid([0; 16])
    }
}

impl fmt::Debug for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        let &Uuid(bytes) = self;
        let id = uuid::Uuid::from_bytes(bytes.clone());
        fmt::Display::fmt(&id, f)
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        let &Uuid(bytes) = self;
        let id = uuid::Uuid::from_bytes(bytes.clone());
        fmt::Display::fmt(&id, f)
    }
}

/// Resets seed and creates a new dataflow graph.
pub fn reset() {
    // All global variables should be reset here.
    RNG.with(|rng| {
        *rng.borrow_mut() = StdRng::from_seed(&[1913, 03, 26]);
    });
    dataflow::graph::default_graph::set(dataflow::graph::Graph::new());
}

lazy_static! {
    static ref TERMINAL_LOGGER: Logger =
        Logger::root(std::sync::Mutex::new(term_full()).fuse(), o!());
}

pub fn get_terminal_logger() -> slog::Logger {
    TERMINAL_LOGGER.clone()
}

pub fn new_app(name: &str) -> clap::App {
    App::new(name)
        .arg(
            Arg::with_name("threads")
                .short("t")
                .long("threads")
                .default_value("4")
                .help("Number of worker threads per process"),
        )
        .arg(
            Arg::with_name("data-addresses")
                .short("d")
                .long("data-addresses")
                .default_value("127.0.0.1:9000")
                .help("Comma separated list of data socket addresses of all nodes"),
        )
        .arg(
            Arg::with_name("control-addresses")
                .short("c")
                .long("control-addresses")
                .default_value("127.0.0.1:9000")
                .help("Comma separated list of control socket addresses of all nodes"),
        )
        .arg(
            Arg::with_name("index")
                .short("i")
                .long("index")
                .default_value("0")
                .help("Current node index"),
        )
        .arg(
            Arg::with_name("graph-filename")
                .short("g")
                .long("graph-filename")
                .default_value("")
                .help("Exports the dataflow graph as a DOT file to the provided filename"),
        )
}
