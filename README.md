# ERDOS

ERDOS is a platform for developing self-driving cars and robotics applications.

[![Crates.io][crates-badge]][crates-url]
[![Build Status](https://github.com/erdos-project/erdos/workflows/CI/badge.svg)](https://github.com/erdos-project/erdos/actions)
[![Documentation Status](https://readthedocs.org/projects/erdos/badge/?version=latest)](https://erdos.readthedocs.io/en/latest/?badge=latest)
[![Documentation](https://docs.rs/erdos/badge.svg)](https://docs.rs/erdos/)

[crates-badge]: https://img.shields.io/crates/v/erdos.svg
[crates-url]: https://crates.io/crates/erdos

# Getting started

The easiest way to get ERDOS running is to use our Docker image:

```console
docker pull erdosproject/erdos
```

# Local installation

## System requirements

ERDOS is known to work on Ubuntu 16.04, 18.04, and 20.04.

## Rust installation

To develop an ERDOS application in Rust, simply include ERDOS in `Cargo.toml`.
The latest ERDOS release is published on
[Crates.io](https://crates.io/crates/erdos)
and documentation is available on [Crates.io](https://crates.io/crates/erdos).

If you'd like to contribute to ERDOS, first
[install Rust](https://www.rust-lang.org/tools/install).
Then run the following to clone the repository and build ERDOS:
```console
rustup default nightly  # use nightly Rust toolchain
git clone https://github.com/erdos-project/erdos.git && cd erdos
cargo build
```

## Python Installation

To develop an ERDOS application in Python, simply run
`pip install erdos`. Documentation is available on
[Read the Docs](https://erdos.readthedocs.io/en/stable/).

If you'd like to contribute to ERDOS, first
[install Rust](https://www.rust-lang.org/tools/install).
Then run the following to clone the repository and build ERDOS:
```console
rustup default nightly  # use nightly Rust toolchain
git clone https://github.com/erdos-project/erdos.git && cd erdos
python3 python/setup.py develop
```

Python files are available under the `python/` directory, and the Python-Rust
bridge interface is developed under `src/python/`.

If you'd like to build ERDOS for release (has better performance, but longer
build times), run `python3 python/setup.py install`.

## Running an example

```console
python3 python/examples/simple_pipeline.py
```

## Build parameters

By default, ERDOS supports up to 20 read streams and 10 write streams in a stream bundle (used to add callbacks across multiple streams; see callback builder in Rust docs for more details).
Some applications may require bundles that support more read and write streams. This can be configured by setting the `ERDOS_BUNDLE_MAX_READ_STREAMS` and `ERDOS_BUNDLE_MAX_WRITE_STREAMS` environment variables when building.

Building ERDOS for the first time may be slow because these parameters result in generated code which dominates the build time.
Subsequent builds should be much faster due to caching unless `build.rs` or the code generation scripts are modified.
In that case, consider setting `ERDOS_BUNDLE_MAX_READ_STREAMS` and `ERDOS_BUNDLE_MAX_WRITE_STREAMS` to speed up builds.

# Writing Applications

ERDOS provides Python and Rust interfaces for developing applications.

The Python interface provides easy integration with popular libraries
such as tensorflow, but comes at the cost of performance
(e.g. slower serialization and the [lack of multithreading](https://wiki.python.org/moin/GlobalInterpreterLock)).

The Rust interface provides more safety guarantees
(e.g. compile-time type checking) and faster performance
(e.g. multithreading and zero-copy message passing).
High performance, safety critical applications such as
self-driving car pipelines deployed in production should use the
Rust API to take full advantage of ERDOS.

# ERDOS Design

ERDOS is a streaming dataflow system designed for self-driving car
pipelines and robotics applications.

Components of the pipelines are implemented as **operators** which
are connected by **data streams**. The set of operators and streams
forms the **dataflow graph**, the representation of the pipline that
ERDOS processes.

Applications define the dataflow graph by connecting operators to streams
in the **driver** section of the program. Operators are typically
implemented elsewhere.

ERDOS is designed for low latency. Self-driving car pipelines require
end-to-end deadlines on the order of hundreds of milliseconds for safe
driving. Similarly, self-driving cars typically process gigabytes per
second of data on small clusters. Therefore, ERDOS is optimized to
send small amounts of data (gigabytes as opposed to terabytes)
as quickly as possible.

ERDOS provides determinisim through **watermarks**. Low watermarks
are a bound on the age of messages received and operators will ignore
any messages older than the most recent watermark received. By processing
on watermarks, applications can avoid non-determinism from processing
messages out of order.

# Getting involved

If you would like to contact us, please send an email to:
erdos-developers@googlegroups.com, or create an issue on GitHub.

We always welcome contributions to ERDOS. One way to get started is to
pick one of the issues tagged with **good first issue** -- these are usually good issues that help you familiarize yourself with the ERDOS
code base. Please submit contributions using pull requests.
