import logging
import multiprocessing as mp
import signal
import sys
from typing import Type, Tuple, Optional

import erdos
from erdos.streams import IngressStream, LoopStream, OperatorStream, Stream
from erdos.internal import PyGraph

logger = logging.getLogger(__name__)


# TODO (Sukrit) : Should this be called a GraphHandle?
# What is the significance of the "Node" here?
class NodeHandle:
    """A handle to the dataflow graph returned by the :py:func:`run_async`
    function.

    The handle exposes functions to :py:func:`shutdown` the dataflow, or
    :py:func:`wait` for its completion.

    Note:
        This structure should not be initialized by the users.
    """
    def __init__(self, py_node_handle, processes):
        self.py_node_handle = py_node_handle
        self.processes = processes

    def shutdown(self):
        """Shuts down the dataflow."""
        logger.info("Shutting down other processes")
        for p in self.processes:
            p.terminate()
            p.join()
        logger.info("Shutting down node.")
        self.py_node_handle.shutdown_node()

    def wait(self):
        """Waits for the completion of all the operators in the dataflow"""
        for p in self.processes:
            p.join()
        logger.debug("Finished waiting for the dataflow graph processes.")


class Graph:
    """An ERDOS dataflow graph representation on which streams and operators
    are added.
    """
    def __init__(self):
        """Constructs a :py:class:`Graph`"""
        self._py_graph = PyGraph()
        self._num_py_operators = 0

    def add_ingress(self, name: str) -> IngressStream:
        return IngressStream(self._py_graph.add_ingress(name))

    def add_loop_stream(self) -> LoopStream:
        return LoopStream(self._py_graph.add_loop_stream())

    def connect_source(
        self,
        op_type: Type[erdos.operator.Source],
        config: erdos.operator.OperatorConfig,
        *args,
        **kwargs,
    ) -> OperatorStream:
        """Registers a :py:class:`.Source` operator to the dataflow
        graph, and returns the :py:class:`OperatorStream` that the operator will
        write the data on.

        Args:
            op_type: The :py:class:`.Source` operator that needs to
                be added to the graph.
            config: Configuration details required by the operator.
            *args: Arguments passed to the operator during initialization.
            **kwargs: Keyword arguments passed to the operator during
                initialization.

        Returns:
            An :py:class:`OperatorStream` corresponding to the
            :py:class:`WriteStream` made available to :py:meth:`.Source.run`.
        """
        if not issubclass(op_type, erdos.operator.Source):
            raise TypeError(
                "{} must subclass erdos.operator.Source".format(op_type))

        if op_type.run.__code__.co_code == erdos.operator.Source.run.__code__.co_code:
            logger.warn("The operator {} does not "
                        "implement the `run` method.".format(op_type))

        # 1-index operators because node 0 is preserved for the current process,
        # and each node can only run 1 python operator.
        self._num_py_operators += 1
        node_id = self._num_py_operators
        logger.debug(
            "Connecting operator #{num} ({name}) to the graph.".format(
                num=node_id, name=config.name))

        internal_stream = self._py_graph.connect_source(
            op_type, config, args, kwargs, node_id)
        return OperatorStream(internal_stream)

    def connect_sink(
        self,
        op_type: Type[erdos.operator.Sink],
        config: erdos.operator.OperatorConfig,
        read_stream: Stream,
        *args,
        **kwargs,
    ):
        """Registers a :py:class:`.Sink` operator to the dataflow
        graph.

        Args:
            op_type: The :py:class:`.Sink` operator that needs to
                be added to the graph.
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

        if not isinstance(read_stream, Stream):
            raise TypeError("{} must subclass `Stream`.".format(read_stream))

        if (op_type.run.__code__.co_code
                == erdos.operator.Sink.run.__code__.co_code
                and op_type.on_data.__code__.co_code
                == erdos.operator.Sink.on_data.__code__.co_code
                and op_type.on_watermark.__code__.co_code
                == erdos.operator.Sink.on_watermark.__code__.co_code):
            logger.warn(
                "The operator {} does not implement any of the "
                "`run`, `on_data` or `on_watermark` methods.".format(op_type))

        # 1-index operators because node 0 is preserved for the current process,
        # and each node can only run 1 python operator.
        self._num_py_operators += 1
        node_id = self._num_py_operators
        logger.debug(
            "Connecting operator #{num} ({name}) to the graph.".format(
                num=node_id, name=config.name))

        self._py_graph.connect_sink(op_type, config,
                                    read_stream._internal_stream, args, kwargs,
                                    node_id)

    def connect_one_in_one_out(
        self,
        op_type: Type[erdos.operator.OneInOneOut],
        config: erdos.operator.OperatorConfig,
        read_stream: Stream,
        *args,
        **kwargs,
    ) -> OperatorStream:
        """Registers a :py:class:`.OneInOneOut` operator to the dataflow graph that
        receives input from the given :code:`read_stream`, and returns the
        :py:class:`OperatorStream` that the operator will write the data on.

        Args:
            op_type: The :py:class:`.OneInOneOut` operator that needs to be added
                to the graph.
            config: Configuration details required by the operator.
            read_stream: The :py:class:`Stream` instance from where the operator
                reads its data.
            *args: Arguments passed to the operator during initialization.
            **kwargs: Keyword arguments passed to the operator during
                initialization.

        Returns:
            An :py:class:`OperatorStream` corresponding to the
            :py:class:`WriteStream` made available to :py:meth:`.OneInOneOut.run`,
            or to the operator's callbacks via the
            :py:class:`.OneInOneOutContext`.
        """
        if not issubclass(op_type, erdos.operator.OneInOneOut):
            raise TypeError(
                "{} must subclass erdos.operator.OneInOneOut".format(op_type))

        if not isinstance(read_stream, Stream):
            raise TypeError("{} must subclass `Stream`.".format(read_stream))

        if (op_type.run.__code__.co_code
                == erdos.operator.OneInOneOut.run.__code__.co_code
                and op_type.on_data.__code__.co_code
                == erdos.operator.OneInOneOut.on_data.__code__.co_code
                and op_type.on_watermark.__code__.co_code
                == erdos.operator.OneInOneOut.on_watermark.__code__.co_code):
            logger.warn(
                "The operator {} does not implement any of the "
                "`run`, `on_data` or `on_watermark` methods.".format(op_type))

        # 1-index operators because node 0 is preserved for the current process,
        # and each node can only run 1 python operator.
        self._num_py_operators += 1
        node_id = self._num_py_operators
        logger.debug(
            "Connecting operator #{num} ({name}) to the graph.".format(
                num=node_id, name=config.name))

        internal_stream = self._py_graph.connect_one_in_one_out(
            op_type, config, read_stream._internal_stream, args, kwargs,
            node_id)
        return OperatorStream(internal_stream)

    def connect_two_in_one_out(
        self,
        op_type: Type[erdos.operator.TwoInOneOut],
        config: erdos.operator.OperatorConfig,
        left_read_stream: Stream,
        right_read_stream: Stream,
        *args,
        **kwargs,
    ) -> OperatorStream:
        """Registers a :py:class:`.TwoInOneOut` operator to the
        dataflow graph that receives input from the given :code:`left_read_stream`
        and :code:`right_read_stream`, and returns the :py:class:`OperatorStream`
        that the operator sends messages on.

        Args:
            op_type: The :py:class:`.TwoInOneOut` operator to add
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
            An :py:class:`OperatorStream` corresponding to the
            :py:class:`WriteStream` made available to :py:meth:`.TwoInOneOut.run`,
            or to the operator's callbacks via the
            :py:class:`.TwoInOneOutContext`.
        """
        if not issubclass(op_type, erdos.operator.TwoInOneOut):
            raise TypeError(
                "{} must subclass erdos.operator.TwoInOneOut".format(op_type))

        if not isinstance(left_read_stream, Stream):
            raise TypeError(
                "{} must subclass `Stream`.".format(left_read_stream))

        if not isinstance(right_read_stream, Stream):
            raise TypeError(
                "{} must subclass `Stream`.".format(right_read_stream))

        if (op_type.run.__code__.co_code
                == erdos.operator.TwoInOneOut.run.__code__.co_code
                and op_type.on_left_data.__code__.co_code
                == erdos.operator.TwoInOneOut.on_left_data.__code__.co_code
                and op_type.on_right_data.__code__.co_code
                == erdos.operator.TwoInOneOut.on_right_data.__code__.co_code
                and op_type.on_watermark.__code__.co_code
                == erdos.operator.TwoInOneOut.on_watermark.__code__.co_code):
            logger.warn("The operator {} does not implement any of the `run`, "
                        "`on_left_data`, `on_right_data` or `on_watermark` "
                        "methods.".format(op_type))

        # 1-index operators because node 0 is preserved for the current process,
        # and each node can only run 1 python operator.
        self._num_py_operators += 1
        node_id = self._num_py_operators
        logger.debug(
            "Connecting operator #{num} ({name}) to the graph.".format(
                num=node_id, name=config.name))

        internal_stream = self._py_graph.connect_two_in_one_out(
            op_type,
            config,
            left_read_stream._internal_stream,
            right_read_stream._internal_stream,
            args,
            kwargs,
            node_id,
        )
        return OperatorStream(internal_stream)

    def connect_one_in_two_out(
        self,
        op_type: Type[erdos.operator.OneInTwoOut],
        config: erdos.operator.OperatorConfig,
        read_stream: Stream,
        *args,
        **kwargs,
    ) -> Tuple[OperatorStream, OperatorStream]:
        """Registers a :py:class:`.OneInTwoOut` operator to the dataflow graph that
        receives input from the given :code:`read_stream`, and returns the pair of
        :py:class:`OperatorStream` instances that the operator will write data on.

        Args:
            op_type: The :py:class:`.OneInTwoOut` operator that needs to be added
                to the graph.
            config: Configuration details required by the operator.
            read_stream: The :py:class:`Stream` instance from where the
                operator reads its data.
            *args: Arguments passed to the operator during initialization.
            **kwargs: Keyword arguments passed to the operator during
                initialization.

        Returns:
            A pair of :py:class:`OperatorStream` instances corresponding to the
            :py:class:`WriteStream` instances made available to
            :py:meth:`.OneInOneOut.run`, or to the operator's callbacks via the
            :py:class:`.OneInTwoOutContext`.
        """
        if not issubclass(op_type, erdos.operator.OneInTwoOut):
            raise TypeError(
                "{} must subclass erdos.operator.OneInTwoOut".format(op_type))

        if not isinstance(read_stream, Stream):
            raise TypeError("{} must subclass `Stream`.".format(read_stream))

        if (op_type.run.__code__.co_code
                == erdos.operator.OneInTwoOut.run.__code__.co_code
                and op_type.on_data.__code__.co_code
                == erdos.operator.OneInTwoOut.on_data.__code__.co_code
                and op_type.on_watermark.__code__.co_code
                == erdos.operator.OneInTwoOut.on_watermark.__code__.co_code):
            logger.warn(
                "The operator {} does not implement any of the "
                "`run`, `on_data` or `on_watermark` methods.".format(op_type))

        # 1-index operators because node 0 is preserved for the current process,
        # and each node can only run 1 python operator.
        self._num_py_operators += 1
        node_id = self._num_py_operators
        logger.debug(
            "Connecting operator #{num} ({name}) to the graph.".format(
                num=node_id, name=config.name))

        left_stream, right_stream = self._py_graph.connect_one_in_two_out(
            op_type, config, read_stream._internal_stream, args, kwargs,
            node_id)
        return OperatorStream(left_stream), OperatorStream(right_stream)

    def run(self,
            graph_filename: Optional[str] = None,
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
        driver_handle = self.run_async(graph_filename, start_port)
        logger.debug("Waiting for the dataflow to complete ...")
        driver_handle.wait()

    def _run_node(self, node_id, data_addresses, control_addresses):
        self._py_graph.run(node_id, data_addresses, control_addresses)

    def run_async(self,
                  graph_filename: Optional[str] = None,
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
            for i in range(self._num_py_operators + 1)
        ]
        control_addresses = [
            "127.0.0.1:{port}".format(port=start_port + len(data_addresses) +
                                      i)
            for i in range(self._num_py_operators + 1)
        ]
        logger.debug("Running the dataflow graph on addresses: {}".format(
            data_addresses))

        # Fix for macOS where mulitprocessing defaults
        # to spawn() instead of fork() in Python 3.8+
        # https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
        # Warning: may lead to crashes
        # https://bugs.python.org/issue33725
        ctx = mp.get_context("fork")
        processes = [
            ctx.Process(target=self._run_node,
                        args=(i, data_addresses, control_addresses))
            for i in range(1, self._num_py_operators + 1)
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
        py_node_handle = self._py_graph.run_async(0, data_addresses,
                                                  control_addresses,
                                                  graph_filename)

        return NodeHandle(py_node_handle, processes)
