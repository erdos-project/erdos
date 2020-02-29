import multiprocessing as mp
import signal
import sys

import erdos.internal as _internal
from erdos.streams import (ReadStream, WriteStream, LoopStream, IngestStream,
                           ExtractStream)
from erdos.operator import Operator, OperatorConfig
from erdos.profile import Profile
from erdos.message import Message, WatermarkMessage
from erdos.timestamp import Timestamp
import erdos.utils

_num_py_operators = 0


def connect(op_type, config, read_streams, *args, **kwargs):
    """Registers the operator and its connected streams on the dataflow graph.

    The operator is created as follows:
    `op_type(*read_streams, *write_streams, *args, **kwargs)`

    Args:
        op_type (type): The operator class. Should inherit from
            `erdos.Operator`.
        config (OperatorConfig): Configuration details required by the
            operator.
        read_streams: the streams from which the operator processes data.
        args: arguments passed to the operator.
        kwargs: keyword arguments passed to the operator.

    Returns:
        write_streams (list of WriteStream): the streams on which the operator
            sends data.
    """
    if not issubclass(op_type, Operator):
        raise TypeError("{} must subclass erdos.Operator".format(op_type))
    # 1-index operators because node 0 is preserved for the current process,
    # and each node can only run 1 python operator.
    global _num_py_operators
    _num_py_operators += 1
    node_id = _num_py_operators

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

    internal_streams = _internal.connect(op_type, config, py_read_streams,
                                         args, kwargs, node_id)
    return [ReadStream(_py_read_stream=s) for s in internal_streams]


def run(graph_filename=None, start_port=9000):
    """Instantiates and runs the dataflow graph.

    ERDOS will spawn 1 process for each python operator, and connect them via
    TCP.

    Args:
        graph_filename (str): the filename to which to write the dataflow graph
            as a DOT file.
        start_port (int): the port on which to start. The start port is the
            lowest port ERDOS will use to establish TCP connections between
            operators.
    """
    data_addresses = [
        "127.0.0.1:{port}".format(port=start_port + i)
        for i in range(_num_py_operators + 1)
    ]
    control_addresses = [
        "127.0.0.1:{port}".format(port=start_port + len(data_addresses) + i)
        for i in range(_num_py_operators + 1)
    ]

    def runner(node_id, data_addresses, control_addresses):
        _internal.run(node_id, data_addresses, control_addresses)

    processes = [
        mp.Process(target=runner, args=(i, data_addresses, control_addresses))
        for i in range(1, _num_py_operators + 1)
    ]

    for p in processes:
        p.start()

    # Needed to shut down child processes
    def sigint_handler(sig, frame):
        for p in processes:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)

    # The driver must always be on node 0 otherwise ingest and extract streams
    # will break
    _internal.run_async(0, data_addresses, control_addresses, graph_filename)

    for p in processes:
        p.join()


def run_async(graph_filename=None, start_port=9000):
    """Instantiates and runs the dataflow graph asynchronously.

    ERDOS will spawn 1 process for each python operator, and connect them via
    TCP.

    Args:
        graph_filename (str): the filename to which to write the dataflow graph
            as a DOT file.
        start_port (int): the port on which to start. The start port is the
            lowest port ERDOS will use to establish TCP connections between
            operators.
    """
    data_addresses = [
        "127.0.0.1:{port}".format(port=start_port + i)
        for i in range(_num_py_operators + 1)
    ]
    control_addresses = [
        "127.0.0.1:{port}".format(port=start_port + len(data_addresses) + i)
        for i in range(_num_py_operators + 1)
    ]

    def runner(node_id, data_addresses, control_addresses):
        _internal.run(node_id, data_addresses, control_addresses)

    processes = [
        mp.Process(target=runner, args=(i, data_addresses, control_addresses))
        for i in range(1, _num_py_operators + 1)
    ]

    # Needed to shut down child processes
    def sigint_handler(sig, frame):
        for p in processes:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)

    for p in processes:
        p.start()

    # The driver must always be on node 0 otherwise ingest and extract streams
    # will break
    _internal.run_async(0, data_addresses, control_addresses, graph_filename)


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


def profile(event_name, operator, event_data=None):
    return Profile(event_name, operator, event_data)
