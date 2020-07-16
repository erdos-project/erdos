import multiprocessing as mp
import inspect
import signal
import sys

from functools import wraps

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
        read_streams (list of ReadStream): ReadStreams corresponding to the
            WriteStreams returned by the operator's connect.
    """
    if not issubclass(op_type, Operator):
        raise TypeError("{} must subclass erdos.Operator".format(op_type))

    # Check if the number of read streams passed are correct.
    required_stream_len = len(inspect.signature(op_type.connect).parameters)
    if not required_stream_len == len(read_streams):
        raise ValueError("{} requires {} streams, but {} were passed.".format(
            op_type.__name__, required_stream_len, len(read_streams)))

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


def reset():
    """Resets internal seed and creates a new dataflow graph.

    Note that no streams or operators can be re-used safely.
    """
    global _num_py_operators
    _num_py_operators = 0
    _internal.reset()


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
    py_node_handle = _internal.run_async(0, data_addresses, control_addresses,
                                         graph_filename)

    return NodeHandle(py_node_handle, processes)


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
    def internal_watermark_callback(py_msg):
        timestamp = Timestamp(coordinates=py_msg.timestamp,
                              is_top=py_msg.is_top_watermark())
        callback(timestamp, *write_streams)

    py_read_streams = [s._py_read_stream for s in read_streams]
    py_write_streams = [s._py_write_stream for s in write_streams]
    _internal.add_watermark_callback(py_read_streams, py_write_streams,
                                     internal_watermark_callback, 0)


def profile(event_name, operator, event_data=None):
    return Profile(event_name, operator, event_data)


def profile_method(**decorator_kwargs):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if isinstance(args[0], Operator):
                # The func is an operator method.
                op_name = args[0].config.name
                cb_name = func.__name__
                if "event_name" in decorator_kwargs:
                    event_name = decorator_kwargs["event_name"]
                else:
                    # Set the event name to the operator name and the callback
                    # name if it's not passed by the user.
                    event_name = op_name + "." + cb_name
                timestamp = None
                if len(args) > 1:
                    if isinstance(args[1], Timestamp):
                        # The func is a watermark callback.
                        timestamp = args[1]
                    elif isinstance(args[1], Message):
                        # The func is a callback.
                        timestamp = args[1].timestamp
            else:
                raise TypeError(
                    "@erdos.profile can only be used on operator methods")

            with erdos.profile(event_name,
                               args[0],
                               event_data={"timestamp": str(timestamp)}):
                func(*args, **kwargs)

        return wrapper

    return decorator


class NodeHandle(object):
    """Used to shutdown a dataflow created by `run_async`."""
    def __init__(self, py_node_handle, processes):
        self.py_node_handle = py_node_handle
        self.processes = processes

    def shutdown(self):
        """Shuts down the dataflow."""
        print("shutting down other processes")
        for p in self.processes:
            p.terminate()
            p.join()
        print("shutting down node")
        self.py_node_handle.shutdown_node()
        print("done shutting down")


__all__ = [
    "ReadStream",
    "WriteStream",
    "LoopStream",
    "IngestStream",
    "ExtractStream",
    "Operator",
    "OperatorConfig",
    "Profile",
    "Message",
    "WatermarkMessage",
    "Timestamp",
    "connect",
    "reset",
    "run",
    "run_async",
    "add_watermark_callback",
    "profile_method",
    "NodeHandle",
]
