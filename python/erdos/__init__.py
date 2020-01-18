import multiprocessing as mp
import signal
import sys

import erdos.internal as _internal
from erdos.streams import (ReadStream, WriteStream, LoopStream, IngestStream,
                           ExtractStream)
from erdos.operator import Operator
from erdos.message import Message, WatermarkMessage
from erdos.timestamp import Timestamp
import erdos.utils

_num_py_operators = 0


def connect(op_type, read_streams, flow_watermarks=True, *args, **kwargs):
    """Registers the operator and its connected streams on the dataflow graph.

    The operator is created as follows:
    `op_type(*read_streams, *write_streams, *args, **kwargs)`

    Args:
        op_type (type): The operator class. Should inherit from
            `erdos.Operator`.
        read_streams: the streams from which the operator processes data.
        flow_watermarks (bool): whether to automatically pass on the low
            watermark.
        args: arguments passed to the operator.
        kwargs: keyword arguments passed to the operator.

    Returns:
        write_streams (list of WriteStream): the streams on which the operator
            sends data.
    """
    global _num_py_operators
    node_id = _num_py_operators
    _num_py_operators += 1

    py_read_streams = []
    for stream in read_streams:
        if isinstance(stream, LoopStream):
            py_read_streams.append(stream._py_loop_stream.to_py_read_stream())
        elif isinstance(stream, IngestStream):
            py_read_streams.append(
                stream._py_ingest_stream.to_py_read_stream())
        elif isinstance(stream, ReadStream):
            py_read_streams.append(stream._py_read_stream)
        else:
            raise TypeError("Unable to convert {stream} to ReadStream".format(
                stream=stream))

    internal_streams = _internal.connect(op_type, py_read_streams, args,
                                         kwargs, node_id, flow_watermarks)
    return [ReadStream(_py_read_stream=s) for s in internal_streams]


def run(driver, start_port=9000):
    """Instantiates and runs the dataflow graph.

    ERDOS will spawn 1 process for each python operator, and connect them via
    TCP.

    Args:
        driver (function): function that builds the dataflow graph. This must
            be passed as a function so it can run on all ERDOS processes.
        start_port (int): the port on which to start. The start port is the
            lowest port ERDOS will use to establish TCP connections between
            operators.
    """
    driver()  # run driver to set _num_py_operators

    addresses = [
        "127.0.0.1:{port}".format(port=start_port + i)
        for i in range(_num_py_operators)
    ]

    def runner(driver, node_id, addresses):
        driver()
        _internal.run(node_id, addresses)

    processes = [
        mp.Process(target=runner, args=(driver, i, addresses))
        for i in range(_num_py_operators)
    ]

    for p in processes:
        p.start()

    # Needed to shut down child processes
    def sigint_handler(sig, frame):
        for p in processes:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)

    for p in processes:
        p.join()


def run_async(driver, start_port=9000):
    """Instantiates and runs the dataflow graph asynchronously.

    ERDOS will spawn 1 process for each python operator, and connect them via
    TCP.

    Args:
        driver (function): function that builds the dataflow graph. This must
            be passed as a function so it can run on all ERDOS processes.
        start_port (int): the port on which to start. The start port is the
            lowest port ERDOS will use to establish TCP connections between
            operators.
    """
    results = driver()  # run driver to set _num_py_operators

    addresses = [
        "127.0.0.1:{port}".format(port=start_port + i)
        for i in range(_num_py_operators + 1)  # Add 1 for the driver
    ]

    def runner(driver, node_id, addresses):
        driver()
        _internal.run(node_id, addresses)

    processes = [
        mp.Process(target=runner,
                   args=(driver, i + 1,
                         addresses))  # Add 1 b/c driver is node 0
        for i in range(_num_py_operators)
    ]

    _internal.run_async(0, addresses)

    for p in processes:
        p.start()

    # Needed to shut down child processes
    def sigint_handler(sig, frame):
        for p in processes:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)

    return results


def add_watermark_callback(read_streams, write_streams, callback):
    """Adds a watermark callback across several read streams.

    Args:
        read_streams (list of ReadStream): streams on which the callback is
            invoked.
        write_streams (list of WriteStream): streams on which the callback
            can send messages.
        callback (timestamp, list of WriteStream -> None): a low watermark
            callback.
    """
    def internal_watermark_callback(coordinates):
        timestamp = Timestamp(coordinates=coordinates)
        callback(timestamp, *write_streams)

    py_read_streams = [s._py_read_stream for s in read_streams]
    _internal.add_watermark_callback(py_read_streams,
                                     internal_watermark_callback)


def _flow_watermark_callback(timestamp, *write_streams):
    """Flows a watermark to all write streams."""
    for write_stream in write_streams:
        write_stream.send(WatermarkMessage(timestamp))
