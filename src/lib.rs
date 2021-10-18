#![doc(html_logo_url = "https://avatars2.githubusercontent.com/u/44586405")]
//! ERDOS is a platform for developing self-driving car and robotics
//! applications. The system is built using techniques from streaming dataflow
//! systems which is reflected by the API.
//!
//! Applications are modeled as directed graphs, in which data flows through
//! [streams](crate::dataflow::stream) and is processed by
//! [operators](crate::dataflow::Operator).
//! Because applications often resemble a sequence of connected operators,
//! an ERDOS application may also be referred to as a *pipeline*.
//!
//! ## Example
//! This example shows an ERDOS application which counts the number of objects
//! detected from a stream of camera frames.
//! The example consists of the [driver](#driver) part of the program, which
//! is responsible for connecting operators via streams.
//! For information on building operators, see [§ Operators](#operators).
//!
//! ```ignore
//! // Capture arguments to set up an ERDOS node.
//! let args = erdos::new_app("ObjectCounter");
//! // Create an ERDOS node which runs the application.
//! let mut node = Node::new(Configuration::from_args(args));
//!
//! // Stream of RGB images from a camera.
//! let camera_frames = erdos::connect_1_write!(
//!     CameraOperator,
//!     OperatorConfig::new().name("Camera")
//! );
//! // Stream of labeled bounding boxes for each RGB image.
//! let detected_objects = erdos::connect_1_write!(
//!     ObjectDetector,
//!     OperatorConfig::new().name("Detector"),
//!     camera_stream
//! );
//! // Stream of detected object count for each RGB image.
//! let num_detected = erdos::connect_1_write!(
//!     MapOperator,
//!     OperatorConfig::new()
//!         .name("Counter")
//!         .arg(|bboxes: &Vec<BBox>| -> usize { bbxes.len() }),
//!     detected_objects
//! );
//!
//! // Run the application
//! node.run();
//! ```
//!
//! ## Driver
//! The *driver* section of the program connects operators together using
//! streams to build an ERDOS application which may then be executed.
//! The driver is typically the `main` function in `main.rs`.
//!
//! The driver may also interact with a running ERDOS application.
//! Using the [`IngestStream`](crate::dataflow::stream::IngestStream),
//! the driver can send data to operators on a stream.
//! The [`ExtractStream`](crate::dataflow::stream::ExtractStream)
//! allows the driver to read data sent from an operator.
//!
//! ## Streams
//! Data is broadcast to all receivers when sending on an ERDOS stream.
//! Streams are typed on their data, and expose 2 classes of interfaces
//! that access the underlying stream:
//! 1. Read-interfaces expose methods to receive and process data.
//!    They allow pulling data by calling
//!    [`read()`](crate::dataflow::stream::ReadStream::read) and
//!    [`try_read()`](crate::dataflow::stream::ReadStream::try_read).
//!    Often, they also support a push data model accessed by registering
//!    callbacks (e.g.
//!    [`add_callback`](crate::dataflow::stream::ReadStream::add_callback) and
//!    [`add_watermark_callback`](crate::dataflow::stream::ReadStream::add_watermark_callback)).
//!    Structures that implement read interfaces include:
//!    - [`ReadStream`](crate::dataflow::stream::ReadStream):
//!      used by operators to read data and register callbacks.
//!    - [`ExtractStream`](crate::dataflow::stream::ExtractStream):
//!      used by the driver to read data.
//! 2. Write-interfaces expose the
//!    [`send`](crate::dataflow::stream::WriteStreamT::send) method to send
//!    data on a stream. Structures that implement write interfaces include:
//!    - [`WriteStream`](crate::dataflow::stream::WriteStream): used by
//!      operators to send data.
//!    - [`IngestStream`](crate::dataflow::stream::IngestStream): used by
//!      the driver to send data.
//!
//! Some applications may want to introduce loops in their dataflow graphs
//! which is possible using the
//! [`LoopStream`](crate::dataflow::stream::LoopStream).
//!
//! ## Operators
//! An ERDOS operator receives data on
//! [`ReadStream`](crate::dataflow::stream::ReadStream)s,
//! and sends processed data on
//! [`WriteStream`](crate::dataflow::stream::WriteStream)s.
//! We provide a [standard library of operators](crate::dataflow::operators)
//! for common dataflow patterns.
//! While the standard operators are general and versatile, some applications
//! may implement custom operators to better optimize performance and take
//! fine-grained control over exection.
//!
//! All operators must implement `new` and `connect` methods, in addition to
//! the [`Operator`](crate::dataflow::Operator) trait.
//! - The `new` method takes an
//!   [`OperatorConfig`](crate::dataflow::OperatorConfig),
//!   all [`ReadStream`](crate::dataflow::stream::ReadStream)s from which the
//!   operator receives data, all
//!   [`WriteStream`](crate::dataflow::stream::WriteStream)s on which the
//!   operator sends data, and returns `Self`.
//!   Within `new`, the state should be initialized, and callbacks may be
//!   registered across [`ReadStream`](crate::dataflow::stream::ReadStream)s.
//! - The `connect` method takes references to the required
//!   [`ReadStream`](crate::dataflow::stream::ReadStream)s
//!   and returns [`WriteStream`](crate::dataflow::stream::WriteStream)s in the
//!   same order as in `new`.
//!
//! For an example, see the implementation of the
//! [`MapOperator`](crate::dataflow::operators::MapOperator).
//!
//! While ERDOS manages the execution of callbacks, some operators require
//! more finegrained control. Operators can take manual control over the
//! thread of execution by overriding the
//! [`run`](crate::dataflow::Operator::run) of the
//! [`Operator`](crate::dataflow::Operator) trait and pulling data from
//! [`ReadStream`](crate::dataflow::stream::ReadStream)s.
//! *Callbacks are not invoked while run executes.*
//!
//! ## Performance
//! ERDOS is designed for low latency. Self-driving car pipelines require
//! end-to-end deadlines on the order of hundreds of milliseconds for safe
//! driving. Similarly, self-driving cars typically process gigabytes per
//! second of data on small clusters. Therefore, ERDOS is optimized to
//! send small amounts of data (gigabytes as opposed to terabytes)
//! as quickly as possible.
//!
//! ## Watermarks
//! Watermarks in ERDOS signal completion of computation. More concretely,
//! sending a watermark with timestamp `t` on a stream asserts that all future
//! messages sent on that stream will have timestamps `t' > t`.
//! ERDOS also introduces a *top watermark*, which is a watermark with the
//! maximum possible timestamp. Sending a top watermark closes the stream as
//! there is no `t' > t_top`, so no more messages can be sent.
//!
//! ## Determinism
//! ERDOS provides mechanisms to enable the building of deterministic
//! applications.
//! For instance, processing sets of messages separated by watermarks using
//! watermark callbacks and
//! [time-versioned state](crate::dataflow::state::TimeVersionedState)
//! turns ERDOS pipelines into
//! [Kahn process networks](https://en.wikipedia.org/wiki/Kahn_process_networks).

// Required for specialization.
#![allow(incomplete_features)]
#![feature(get_mut_unchecked)]
#![feature(specialization)]
#![feature(box_into_pin)]

// Re-exports of libraries used in macros.
#[doc(hidden)]
pub use ::slog;
#[doc(hidden)]
pub use ::tokio;

// Libraries used in this file.
use std::{cell::RefCell, fmt};

use abomonation_derive::Abomonation;
use clap::{self, App, Arg};
use lazy_static::lazy_static;
use rand::{Rng, SeedableRng, StdRng};
use serde::{Deserialize, Serialize};
use slog::{Drain, Logger};
use slog_term::{self, term_full};
use uuid;

// Private submodules
mod configuration;
#[cfg(feature = "python")]
mod python;

// Public submodules
#[doc(hidden)]
pub mod communication;
pub mod dataflow;
pub mod node;
#[doc(hidden)]
pub mod scheduler;

// Public exports
pub use configuration::Configuration;
pub use dataflow::{connect::*, OperatorConfig};

/// A unique identifier for an operator.
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

/// Wrapper around [`uuid::Uuid`] that implements [`Abomonation`](abomonation::Abomonation) for fast serialization.
#[derive(
    Abomonation, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
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
    dataflow::graph::default_graph::set(dataflow::graph::AbstractGraph::new());
}

lazy_static! {
    static ref TERMINAL_LOGGER: Logger =
        Logger::root(std::sync::Mutex::new(term_full()).fuse(), slog::o!());
}

/// Returns a logger that prints messages to the console.
pub fn get_terminal_logger() -> slog::Logger {
    TERMINAL_LOGGER.clone()
}

/// Defines command line arguments for running a multi-node ERDOS application.
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
