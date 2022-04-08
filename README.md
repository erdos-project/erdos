# ERDOS

ERDOS is a platform for developing self-driving cars and robotics applications.

[![Crates.io][crates-badge]][crates-url]
[![Build Status](https://github.com/erdos-project/erdos/workflows/CI/badge.svg)](https://github.com/erdos-project/erdos/actions)
[![Documentation Status](https://readthedocs.org/projects/erdos/badge/?version=latest)](https://erdos.readthedocs.io/en/latest/?badge=latest)
[![Documentation](https://docs.rs/erdos/badge.svg)](https://docs.rs/erdos/)

[crates-badge]: https://img.shields.io/crates/v/erdos.svg
[crates-url]: https://crates.io/crates/erdos

# Getting started

# Local installation

## System requirements

ERDOS is known to work on Ubuntu 18.04 and 20.04.

## Rust installation

To develop an ERDOS application in Rust, simply include ERDOS in `Cargo.toml`.
The latest ERDOS release is published on
[Crates.io](https://crates.io/crates/erdos)
and documentation is available on [Docs.rs](https://docs.rs/erdos).

If you'd like to contribute to ERDOS, first
[install Rust](https://www.rust-lang.org/tools/install).
Then run the following to clone the repository and build ERDOS:
```console
git clone https://github.com/erdos-project/erdos.git && cd erdos
cargo build
```

## Python Installation

To develop an ERDOS application in Python, simply run
`pip install erdos`. Documentation is available on
[Read the Docs](https://erdos.readthedocs.io/).

If you'd like to contribute to ERDOS, first
[install Rust](https://www.rust-lang.org/tools/install).
Within a [virtual environment](https://docs.python.org/3/tutorial/venv.html),
run the following to clone the repository and build ERDOS:
```console
git clone https://github.com/erdos-project/erdos.git && cd erdos/python
pip3 install maturin
maturin develop
```

The Python-Rust bridge interface is developed in the `python` crate, which
also contains user-facing python files under the `python/erdos` directory.

If you'd like to build ERDOS for release (better performance, but longer
build times), run `maturin develop --release`.

## Running an example

```console
python3 python/examples/simple_pipeline.py
```

# Writing Applications

ERDOS provides Python and Rust interfaces for developing applications.

The Python interface provides easy integration with popular libraries
such as tensorflow, but comes at the cost of performance
(e.g. slower serialization and the [lack of parallelism within a process](https://wiki.python.org/moin/GlobalInterpreterLock)).

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

ERDOS provides determinism through **watermarks**. Low watermarks
are a bound on the age of messages received and operators will ignore
any messages older than the most recent watermark received. By processing
on watermarks, applications can avoid non-determinism from processing
messages out of order.

To read more about the ideas behind ERDOS, refer to our paper,
[*D3: A Dynamic Deadline-Driven Approach for Building Autonomous Vehicles*](https://dl.acm.org/doi/10.1145/3492321.3519576).
If you find ERDOS useful to your work, please consider citing our paper:
```bibtex
@inproceedings{gog2022d3,
  title={D3: a dynamic deadline-driven approach for building autonomous vehicles},
  author={Gog, Ionel and Kalra, Sukrit and Schafhalter, Peter and Gonzalez, Joseph E and Stoica, Ion},
  booktitle={Proceedings of the Seventeenth European Conference on Computer Systems},
  pages={453--471},
  year={2022}
}
```

# Pylot

We are actively developing an AV platform atop ERDOS! For more information, see the [Pylot repository](https://github.com/erdos-project/pylot/).

# Getting involved

If you would like to contact us, you can:
* [Community on Slack](https://forms.gle/KXwSrjM6ZqRi2MT18): Join our community
on Slack for discussions about development, questions about usage, and feature
requests.
* [Github Issues](https://github.com/erdos-project/erdos/issues): For reporting
bugs.

We always welcome contributions to ERDOS. One way to get started is to
pick one of the issues tagged with **good first issue** -- these are usually good issues that help you familiarize yourself with the ERDOS
code base. Please submit contributions using pull requests.
