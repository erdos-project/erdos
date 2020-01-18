# ERDOS Python Interface

## Building Wheels

Use the [manylinux1](https://github.com/pypa/manylinux) docker container
to generate wheels for ERDOS.

Navigate to the project root and run the following command to build ERDOS
wheels for x86-64:

```console
docker run -it -v $(pwd):/io quay.io/pypa/manylinux1_x86_64 bash io/python/build-wheels.sh
```

The wheels will appear in the `dist` folder in the project root.
