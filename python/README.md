# ERDOS Python Interface

## Building from Source

First, run `pip install maturin` to install [Maturin](https://github.com/PyO3/maturin),
which aids in building Rust crates with Python bindings.

### Debug Build

From the `python/` directory, run `maturin develop`.

This will build the debug version of the rust backend resulting in more debug messages, faster compile times, and slower performance.

### Release Build

Run `maturin develop --release`.

This will build the release version of the Rust backend resulting in less debug messages, slower compile times, and faster performance.

## Building Wheels

To generate wheels, run `maturin build`.

This will generate wheels and store them in `target/wheels`.

For more information, including on building portable wheels that comply with manylinux, see [Maturin's README](https://github.com/PyO3/maturin#manylinux-and-auditwheel) and the [Maturin guide](https://maturin.rs/distribution.html).