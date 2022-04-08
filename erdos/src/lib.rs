#![doc(html_logo_url = "https://avatars2.githubusercontent.com/u/44586405")]
//! ERDOS is a platform for developing self-driving car and robotics
//! applications. The system is built using techniques from streaming dataflow
//! systems which is reflected by the API.
//!
//! Applications are modeled as directed graphs, in which data flows through
//! [streams](crate::dataflow::stream) and is processed by
//! [operators](crate::dataflow::operator).
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
//! let mut node = Node::new(Configuration::from_args(&args));
//!
//! // Stream of RGB images from a camera.
//! let camera_frames = erdos::connect_source(
//!     CameraOperator::new,
//!     OperatorConfig::new().name("Camera")
//! );
//! // Stream of labeled bounding boxes for each RGB image.
//! let detected_objects = erdos::connect_one_in_one_out(
//!     ObjectDetector::new,
//!     || {},
//!     OperatorConfig::new().name("Detector"),
//!     &camera_frames
//! );
//! // Stream of detected object count for each RGB image.
//! let num_detected = erdos::connect_one_in_one_out(
//!     || { MapOperator::new(|bboxes: &Vec<BBox>| -> usize { bboxes.len() }) },
//!     || {},
//!     OperatorConfig::new().name("Counter"),
//!     &detected_objects
//! );
//!
//! // Run the application
//! node.run();
//! ```
//!
//! ## Operators
//! ERDOS operators process received data, and use
//! [streams](crate::dataflow::stream) to broadcast
//! [`Message`s](crate::dataflow::Message) to downstream operators.
//! ERDOS provides a [standard library of operators](crate::dataflow::operators)
//! for common dataflow patterns.
//! While the standard operators are general and versatile, some applications
//! may implement custom operators to better optimize performance and take
//! fine-grained control over exection.
//!
//! ### Implementing Operators
//! For an example, see the implementation of the
//! [`FlatMapOperator`](crate::dataflow::operators::FlatMapOperator).
//!
//! Operators are structures which implement an
//! [operator trait](crate::dataflow::operator) reflecting their
//! communication pattern.
//! For example, the [`SplitOperator`](crate::dataflow::operators::SplitOperator)
//! implements [`OneInTwoOut`](crate::dataflow::operator::OneInTwoOut)
//! because it receives data on one input stream, and sends messages on
//! two output streams.
//!
//! Operators can support both push and pull-based models of execution
//! by implementing methods defined in the
//! [operator traits](crate::dataflow::operator).
//! By implementing callbacks such as
//! [`OneInOneOut::on_data`](crate::dataflow::operator::OneInOneOut::on_data),
//! operators can process messages as they arrive.
//! Moreover, operators can implement callbacks over [watermarks](#watermarks)
//! (e.g. [`OneInOneOut::on_watermark`](crate::dataflow::operator::OneInOneOut::on_watermark))
//! to ensure ordered processing over timestamps.
//! ERDOS ensures lock-free, safe, and concurrent processing by ordering
//! callbacks in an ERDOS-managed execution lattice, which serves as a run
//! queue for the system's multithreaded runtime.
//!
//! While ERDOS manages the execution of callbacks, some operators require
//! more finegrained control. Operators can use the pull-based model
//! to take over the thread of execution by overriding the `run` method
//! (e.g. [`OneInOneOut::run`](crate::dataflow::operator::OneInOneOut::run))
//! of an [operator trait](crate::dataflow::operator), and pulling data from
//! the [`ReadStream`](crate::dataflow::stream::ReadStream)s.
//! *Callbacks are not invoked while `run` executes.*
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
//! watermark callbacks can turn ERDOS pipelines into
//! [Kahn process networks](https://en.wikipedia.org/wiki/Kahn_process_networks).
//!
//! ## More Information
//! To read more about the ideas behind ERDOS, refer to our paper
//! [*D3: A Dynamic Deadline-Driven Approach for Building Autonomous Vehicles*](https://dl.acm.org/doi/10.1145/3492321.3519576).
//! If you find ERDOS useful to your work, please consider citing our paper:
//! ```bibtex
//! @inproceedings{gog2022d3,
//!   title={D3: a dynamic deadline-driven approach for building autonomous vehicles},
//!   author={Gog, Ionel and Kalra, Sukrit and Schafhalter, Peter and Gonzalez, Joseph E and Stoica, Ion},
//!   booktitle={Proceedings of the Seventeenth European Conference on Computer Systems},
//!   pages={453--471},
//!   year={2022}
//! }
//! ```

// Required for specialization.
#![allow(incomplete_features)]
#![feature(specialization)]
#![feature(box_into_pin)]

// Re-exports of libraries used in macros.
#[doc(hidden)]
pub use ::tokio;

// Libraries used in this file.
use std::{cell::RefCell, fmt};

use abomonation_derive::Abomonation;
use clap::{self, App, Arg};
use rand::{Rng, SeedableRng, StdRng};
use serde::{Deserialize, Serialize};

// Private submodules
mod configuration;

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
thread_local!(static RNG: RefCell<StdRng>= RefCell::new(StdRng::from_seed(&[1913, 3, 26])));

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
        let id = uuid::Uuid::from_bytes(bytes);
        fmt::Display::fmt(&id, f)
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        let &Uuid(bytes) = self;
        let id = uuid::Uuid::from_bytes(bytes);
        fmt::Display::fmt(&id, f)
    }
}

/// Resets seed and creates a new dataflow graph.
pub fn reset() {
    // All global variables should be reset here.
    RNG.with(|rng| {
        *rng.borrow_mut() = StdRng::from_seed(&[1913, 3, 26]);
    });
    dataflow::graph::default_graph::set(dataflow::graph::AbstractGraph::new());
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
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .multiple(true)
                .takes_value(false)
                .help("Sets the level of verbosity"),
        )
}
