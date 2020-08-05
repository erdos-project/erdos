import inspect
import logging
import multiprocessing as mp
import signal
import sys

from functools import wraps
from operator import attrgetter
from typing import Type, List, Optional, Callable

import erdos.internal as _internal
from erdos.streams import (ReadStream, WriteStream, LoopStream, IngestStream,
                           ExtractStream)
from erdos.operator import Operator, OperatorConfig
from erdos.profile import Profile
from erdos.message import Message, WatermarkMessage
from erdos.timestamp import Timestamp
import erdos.utils

_num_py_operators = 0

# Set the top-level logger for ERDOS logging.
# Users can change the logging level to the required level by calling setLevel
# erdos.logger.setLevel(logging.DEBUG)
FORMAT = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d,%H:%M:%S"
formatter = logging.Formatter(FORMAT, datefmt=DATE_FORMAT)
default_handler = logging.StreamHandler(sys.stderr)
default_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(default_handler)
logger.setLevel(logging.WARNING)
logger.propagate = False


def connect(
    op_type: Type[Operator],
    config: OperatorConfig,
    read_streams: List[ReadStream],
    *args,
    **kwargs,
) -> List[ReadStream]:
    """Registers the operator and its connected streams on the dataflow graph.

    This function performs initialization of the operator by registering its
    read and write stream dependencies with the internal graph representation.
    It is not sufficient to call :py:func:`Operator.connect` in the driver.

    The `read_streams` are passed to the `connect` function of `op_type` to
    retrieve the write streams of the operator. The operator is then
    initialized with the given `read_streams` and the returned `write_streams`
    after calling :py:func:`run`:
        >>> write_streams = op_type.connect(*read_streams)
        >>> op_type(*read_streams, *write_streams, *args, **kwargs)

    Args:
        op_type: The :py:class:`.Operator` that needs to be added to the graph
            with the corresponding `read_streams`.
        config: Configuration details required by the operator.
        read_streams: The streams on which the operator processes data.
        *args: Arguments passed to the operator during initialization.
        **kwargs: Keyword arguments passed to the operator during
            initialization.
    Returns:
        A list of :py:class:`.ReadStream` s corresponding to the
        :py:class:`.WriteStream` s returned by the `connect` function of the
        :py:class:`.Operator`.
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
    logger.debug("Connecting operator #{num} ({name}) to the graph.".format(
        num=node_id, name=config.name))

    py_read_streams = []
    op_read_streams = list(
        inspect.signature(op_type.connect).parameters.keys())
    for index, stream in enumerate(read_streams):
        logger.debug("Passing the stream {} to operator {}.".format(
            op_read_streams[index], config.name))
        if isinstance(stream, LoopStream):
            py_read_streams.append(stream._py_loop_stream.to_py_read_stream())
        elif isinstance(stream, IngestStream):
            py_read_streams.append(
                stream._py_ingest_stream.to_py_read_stream())
        elif isinstance(stream, ReadStream):
            py_read_streams.append(stream._py_read_stream)
        else:
            raise TypeError(
                "Unable to convert {stream} of type {stream_type} to "
                "ReadStream".format(stream=stream, stream_type=type(stream)))

    internal_streams = _internal.connect(op_type, config, py_read_streams,
                                         args, kwargs, node_id)
    logger.debug("Converting {} write stream(s) returned by "
                 "operator {} to read stream(s).".format(
                     len(internal_streams), config.name))
    return [ReadStream(_py_read_stream=s) for s in internal_streams]


def reset():
    """Create a new dataflow graph.

    Note:
        A call to this function renders the previous dataflow graph unsafe to
        use.
    """
    logger.info("Resetting the default graph.")
    global _num_py_operators
    _num_py_operators = 0
    _internal.reset()


# TODO (Sukrit) : Should this be called a GraphHandle?
# What is the significance of the "Node" here?
class NodeHandle(object):
    """ A handle to the dataflow graph returned by the :py:func:`run_async`
    method.

    The handle exposes functions to :py:func:`shutdown` the dataflow, or
    :py:func:`wait` for its completion.

    Note:
        This structure should not be initialized by the users.
    """
    def __init__(self, py_node_handle, processes):
        self.py_node_handle = py_node_handle
        self.processes = processes

    def shutdown(self):
        """ Shuts down the dataflow."""
        logger.info("Shutting down other processes")
        for p in self.processes:
            p.terminate()
            p.join()
        logger.info("Shutting down node.")
        self.py_node_handle.shutdown_node()

    def wait(self):
        """ Waits for the completion of all the operators in the dataflow """
        for p in self.processes:
            p.join()
        logger.debug("Finished waiting for the dataflow graph processes.")


def run(graph_filename: Optional[str] = None,
        start_port: Optional[int] = 9000):
    """Instantiates and runs the dataflow graph.

    ERDOS will spawn 1 process for each python operator, and connect them via
    TCP.

    Args:
        graph_filename: The filename to which to write the dataflow graph
            as a DOT file.
        start_port: The port on which to start. The start port is the
            lowest port ERDOS will use to establish TCP connections between
            operators.
    """
    driver_handle = run_async(graph_filename, start_port)
    logger.debug("Waiting for the dataflow to complete ...")
    driver_handle.wait()


def run_async(graph_filename: Optional[str] = None,
              start_port: Optional[int] = 9000) -> NodeHandle:
    """Instantiates and runs the dataflow graph asynchronously.

    ERDOS will spawn 1 process for each python operator, and connect them via
    TCP.

    Args:
        graph_filename: The filename to which to write the dataflow graph
            as a DOT file.
        start_port: The port on which to start. The start port is the
            lowest port ERDOS will use to establish TCP connections between
            operators.

    Returns:
        A :py:class:`.NodeHandle` that allows the driver to interface with the
        dataflow graph.
    """
    data_addresses = [
        "127.0.0.1:{port}".format(port=start_port + i)
        for i in range(_num_py_operators + 1)
    ]
    control_addresses = [
        "127.0.0.1:{port}".format(port=start_port + len(data_addresses) + i)
        for i in range(_num_py_operators + 1)
    ]
    logger.debug(
        "Running the dataflow graph on addresses: {}".format(data_addresses))

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


def add_watermark_callback(read_streams: List[erdos.ReadStream],
                           write_streams: List[erdos.WriteStream],
                           callback: Callable):
    """Adds a watermark callback across several read streams.

    Args:
        read_streams: Streams on which the callback is invoked.
        write_streams: Streams on which the callback can send messages.
        callback: The callback to be invoked upon receipt of a
            :py:class:`.WatermarkMessage` on all the `read_streams`.
    """
    logger.debug("Adding watermark callback {name} to the input streams: "
                 "{_input}, and passing the output streams: {_output}".format(
                     name=callback.__name__,
                     _input=list(map(attrgetter("_name"), read_streams)),
                     _output=list(map(attrgetter("_name"), write_streams))))

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
                return func(*args, **kwargs)

        return wrapper

    return decorator


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
