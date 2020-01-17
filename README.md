# ERDOS

ERDOS is a platform for developing self-driving cars and robotics applications.

[![Crates.io][crates-badge]][crates-url]
[![Build Status](https://travis-ci.org/erdos-project/erdos.svg)](https://travis-ci.org/erdos-project/erdos)
[![Documentation Status](https://readthedocs.org/projects/erdos/badge/?version=latest)](https://erdos.readthedocs.io/en/latest/?badge=latest)
[![Documentation](https://docs.rs/erdos/badge.svg)](https://docs.rs/erdos/0.2.0/erdos/)

[crates-badge]: https://img.shields.io/crates/v/erdos.svg
[crates-url]: https://crates.io/crates/erdos

# Getting started

The easiest way to get ERDOS running is to use our Docker image:

```console
docker pull erdosproject/erdos
```

# Local installation

## System requirements

ERDOS is known to work on Ubuntu 16.04, 18.04, and 19.10.

## Installation

```console
git clone https://github.com/erdos-project/erdos.git && cd erdos

python3 python/setup.py develop
```

This script installs Python and Rust dependencies in addition to ERDOS.

## Running an example

```console
python3 python/examples/simple_pipeline.py
```

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
