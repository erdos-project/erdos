import logging
import multiprocessing as mp
import signal
import sys

from functools import wraps
from typing import Type, Optional

import erdos.internal as _internal
from erdos.streams import (ReadStream, WriteStream, LoopStream, IngestStream,
                           ExtractStream, Stream)
from erdos.profile import Profile
from erdos.message import Message, WatermarkMessage
from erdos.timestamp import Timestamp
import erdos.utils
import erdos.operator
import erdos.context

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


def connect_source(
    op_type: Type[erdos.operator.Source],
    config: erdos.operator.OperatorConfig,
    *args,
    **kwargs,
) -> Stream:
    """Registers a :py:class:`Source` operator to the dataflow graph, and
    returns the :py:class:`Stream` that the operator will write the data on.

    Args:
        op_type: The :py:class:`Source` operator that needs to be added to the
            graph.
        config: Configuration details required by the operator.
        *args: Arguments passed to the operator during initialization.
        **kwargs: Keyword arguments passed to the operator during
            initialization.

    Returns:
        A :py:class:`Stream` corresponding to the :py:class:`WriteStream` made
        available to the operator's :py:func:`run` method.
    """
    if not issubclass(op_type, erdos.operator.Source):
        raise TypeError(
                "{} must subclass erdos.operator.Source".format(op_type))

    # 1-index operators because node 0 is preserved for the current process,
    # and each node can only run 1 python operator.
    global _num_py_operators
    _num_py_operators += 1
    node_id = _num_py_operators
    logger.debug("Connecting operator #{num} ({name}) to the graph.".format(
        num=node_id, name=config.name))

    internal_stream = _internal.connect_source(op_type, config, args, kwargs,
                                               node_id)
    return Stream(_py_stream=internal_stream)


def connect_sink(
    op_type: Type[erdos.operator.Sink],
    config: erdos.operator.OperatorConfig,
    read_stream: Stream,
    *args,
    **kwargs,
):
    """Registers a :py:class:`Sink` operator to the dataflow graph.

    Args:
        op_type: The :py:class:`Sink` operator that needs to be added to the
            graph.
        config: Configuration details required by the operator.
        read_stream: The :py:class:`Stream` instance from where the operator
            reads its data.
        *args: Arguments passed to the operator during initialization.
        **kwargs: Keyword arguments passed to the operator during
            initialization.
    """
    if not issubclass(op_type, erdos.operator.Sink):
        raise TypeError(
                "{} must subclass erdos.operator.Sink".format(op_type))

    # 1-index operators because node 0 is preserved for the current process,
    # and each node can only run 1 python operator.
    global _num_py_operators
    _num_py_operators += 1
    node_id = _num_py_operators
    logger.debug("Connecting operator #{num} ({name}) to the graph.".format(
        num=node_id, name=config.name))

    _internal.connect_sink(op_type, config, read_stream._py_stream,
                           args, kwargs, node_id)


def connect_one_in_one_out(
    op_type: Type[erdos.operator.OneInOneOut],
    config: erdos.operator.OperatorConfig,
    read_stream: Stream,
    *args,
    **kwargs,
) -> Stream:
    """Registers a :py:class:`OneInOneOut` operator to the dataflow graph that
    receives input from the given `read_stream`, and returns the
    :py:class:`Stream` that the operator will write the data on.

    Args:
        op_type: The :py:class:`OneInOneOut` operator that needs to be added
            to the graph.
        config: Configuration details required by the operator.
        read_stream: The :py:class:`Stream` instance from where the operator
            reads its data.
        *args: Arguments passed to the operator during initialization.
        **kwargs: Keyword arguments passed to the operator during
            initialization.

    Returns:
        A :py:class:`Stream` corresponding to the :py:class:`WriteStream` made
        available to the operator's :py:func:`run` method.
    """
    if not issubclass(op_type, erdos.operator.OneInOneOut):
        raise TypeError(
                "{} must subclass erdos.operator.OneInOneOut".format(op_type))

    # 1-index operators because node 0 is preserved for the current process,
    # and each node can only run 1 python operator.
    global _num_py_operators
    _num_py_operators += 1
    node_id = _num_py_operators
    logger.debug("Connecting operator #{num} ({name}) to the graph.".format(
        num=node_id, name=config.name))

    internal_stream = _internal.connect_one_in_one_out(
                            op_type,
                            config,
                            read_stream._py_stream,
                            args, kwargs,
                            node_id)
    return Stream(_py_stream=internal_stream)


def connect_two_in_one_out(
    op_type: Type[erdos.operator.TwoInOneOut],
    config: erdos.operator.OperatorConfig,
    left_read_stream: Stream,
    right_read_stream: Stream,
    *args,
    **kwargs,
) -> Stream:
    """Registers a :py:class:`TwoInOneOut` operator to the dataflow graph that
    receives input from the given `left_read_stream` and `right_read_stream`,
    and returns the :py:class:`Stream` that the operator will write data on.

    Args:
        op_type: The :py:class:`TwoInOneOut` operator that needs to be added
            to the graph.
        config: Configuration details required by the operator.
        left_read_stream: The first :py:class:`Stream` instance from where the
            operator reads its data.
        right_read_stream: The second :py:class:`Stream` instance from where
            the operator reads its data.
        *args: Arguments passed to the operator during initialization.
        **kwargs: Keyword arguments passed to the operator during
            initialization.

    Returns:
        A :py:class:`Stream` corresponding to the :py:class:`WriteStream` made
        available to the operator's :py:func:`run` method.
    """
    if not issubclass(op_type, erdos.operator.TwoInOneOut):
        raise TypeError(
                "{} must subclass erdos.operator.TwoInOneOut".format(op_type))

    # 1-index operators because node 0 is preserved for the current process,
    # and each node can only run 1 python operator.
    global _num_py_operators
    _num_py_operators += 1
    node_id = _num_py_operators
    logger.debug("Connecting operator #{num} ({name}) to the graph.".format(
        num=node_id, name=config.name))

    internal_stream = _internal.connect_two_in_one_out(
                            op_type,
                            config,
                            left_read_stream._py_stream,
                            right_read_stream._py_stream,
                            args, kwargs,
                            node_id)
    return Stream(_py_stream=internal_stream)


def connect_one_in_two_out(
    op_type: Type[erdos.operator.OneInTwoOut],
    config: erdos.operator.OperatorConfig,
    read_stream: Stream,
    *args,
    **kwargs,
) -> (Stream, Stream):
    """Registers a :py:class:`OneInTwoOut` operator to the dataflow graph that
    receives input from the given `read_stream`, and returns the pair of
    :py:class:`Stream` instances that the operator will write data on.

    Args:
        op_type: The :py:class:`OneInTwoOut` operator that needs to be added
            to the graph.
        config: Configuration details required by the operator.
        read_stream: The :py:class:`Stream` instance from where the
            operator reads its data.
        *args: Arguments passed to the operator during initialization.
        **kwargs: Keyword arguments passed to the operator during
            initialization.

    Returns:
        A pair of :py:class:`Stream`s corresponding to the
        :py:class:`WriteStream`s made available to the operator's
        :py:func:`run` method.
    """
    if not issubclass(op_type, erdos.operator.OneInTwoOut):
        raise TypeError(
                "{} must subclass erdos.operator.OneInTwoOut".format(op_type))

    # 1-index operators because node 0 is preserved for the current process,
    # and each node can only run 1 python operator.
    global _num_py_operators
    _num_py_operators += 1
    node_id = _num_py_operators
    logger.debug("Connecting operator #{num} ({name}) to the graph.".format(
        num=node_id, name=config.name))

    left_stream, right_stream = _internal.connect_one_in_two_out(
                                        op_type,
                                        config,
                                        read_stream._py_stream,
                                        args, kwargs,
                                        node_id)
    return Stream(_py_stream=left_stream), Stream(_py_stream=right_stream)


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


def profile(event_name, operator, event_data=None):
    return Profile(event_name, operator, event_data)


def profile_method(**decorator_kwargs):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if isinstance(args[0], erdos.operator.BaseOperator):
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
    "Profile",
    "Message",
    "WatermarkMessage",
    "Timestamp",
    "connect_source",
    "connect_sink",
    "reset",
    "run",
    "run_async",
    "profile_method",
    "NodeHandle",
]
