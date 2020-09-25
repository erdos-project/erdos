#!/bin/bash
set -ex

curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain nightly-2020-06-22 --profile minimal -y
source $HOME/.cargo/env

if [ "$1" == github-actions ]; then
    cd /github/workspace/
else
    cd /io
fi

for PYBIN in /opt/python/{cp35-cp35m,cp36-cp36m,cp37-cp37m,cp38-cp38}/bin; do
    # Temporarily add the python directory to the path so the ERDOS build doesn't
    # error when it calls python3 in build.rs.
    PREV_PATH=$PATH
    PATH="$PYBIN:$PATH"
    "${PYBIN}/pip" install -U setuptools wheel setuptools-rust
    "${PYBIN}/python" python/setup.py bdist_wheel
    PATH=$PREV_PATH
done

# Update auditwheel on python3.8 to avoid errors.
/opt/python/cp38-cp38/bin/pip install -U auditwheel

for whl in dist/*.whl; do
    /opt/python/cp38-cp38/bin/python -m auditwheel repair "$whl" -w dist/
done
