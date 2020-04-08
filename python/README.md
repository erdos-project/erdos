# ERDOS Python Interface

## Building from Source

### Debug Build

From the root of the project, run `python3 python/setup.py develop`.

This will build the debug version of the rust backend resulting in more debug messages, faster compile times, and slower performance.

For some setups, you may need to add the `--user` flag.

### Release Build

From the root of the project, run `python3 python/setup.py install`.

This will build the release version of the Rust backend resulting in less debug messages, slower compile times, and faster performance.

For some setups, you may need to add the `--user` flag.

## Building Wheels

Use the [manylinux1](https://github.com/pypa/manylinux) docker container
to generate wheels for ERDOS.

Navigate to the project root and run the following command to build ERDOS
wheels for x86-64:

```console
docker run -it -v $(pwd):/io quay.io/pypa/manylinux1_x86_64 bash io/python/build-wheels.sh
```

The wheels will appear in the `dist` folder in the project root.
