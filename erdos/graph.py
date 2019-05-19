from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess
import time
from absl import flags

from erdos.op_handle import OpHandle
from erdos.graph_handle import GraphHandle
from erdos.data_streams import DataStreams
from erdos.local.local_executor import LocalExecutor
from erdos.utils import log_graph_to_dot_file
from erdos.operators import NoopOp

FLAGS = flags.FLAGS
flags.DEFINE_string('ray_redis_address', '', 'Address of the Ray redis master')
flags.DEFINE_bool('log_graph', False, 'True to enable graph dot file logging')
# XXX(ionel): We can't put ros_non_dropping in ros_input_data_stream because
# the file is conditionally imported.
flags.DEFINE_bool(
    'ros_non_dropping', True,
    'Use an infinite ROS receive buffer so that no messages are dropped')


class Graph(object):
    """An execution graph consisting of operators joined by data streams.

    The graph begins operator execution so that downstream operators that
    receive data always start before upstream operators that send data.

    Cyclical graphs are supported.

    By default, each graph generates an input operator and an output operator
    which are used to implement `graph.construct`.

    Attributes:
        operators (dict of str -> Op): A mapping of operator name to operator.
        framework (str): The name of the framework to use to execute the
            operators. Either ROS or Ray.
        parent (Graph): This graph's parent graph. None if this graph has no
            parent.
    """

    def __init__(self, name="default", parent=None):
        self.graph_name = name if name else "{0}_{1}".format(
            self.__class__.__name__, hash(self))
        self.parent = parent
        self.op_handles = {}
        self.graph_handles = {}
        self.output_stream_to_op_id_sinks = {}
        self.framework = "ray"

        # TODO(peter): fix this once the nested graph API switches to setup_streams
        self.input_op = self.add(NoopOp, name='input_op')
        self.output_op = self.add(NoopOp, name='output_op')

    def add(self, op_cls, name="", init_args=None, setup_args=None,
            input_streams=[], _resources=None):
        """Adds an operator to the execution graph.

        Args:
            op_cls (type): Type of the operator to add to the graph. Must
                inherit from `Op`.
            name (str): Operator name.
            init_args (dict): Arguments passed to the operator's `__init__`
                method.
            setup_args (dict): Arguments passed to the operator's
                `setup_streams` method.

        Returns:
            (str): Unique operator identifier.
        """
        if issubclass(op_cls, Graph):
            handle = GraphHandle(name, op_cls, init_args, setup_args,
                                 self.graph_name, self)
        else:
            handle = OpHandle(name, op_cls, init_args, setup_args,
                              self.graph_name, resources = _resources)
        op_id = handle.get_uid()
        assert (op_id not in self.op_handles), \
            'Duplicate operator name {}. Ensure name uniqueness ' \
            'or do not operator specify name'.format(handle.name)
        # XXX(ionel): Hack so that we can feed in data from drivers into
        # operators.
        handle.driver_input_streams = input_streams
        # Deep copy streams.
        handle.input_streams = input_streams[:]
        if issubclass(op_cls, Graph):
            self.graph_handles[op_id] = handle
        self.op_handles[op_id] = handle
        return op_id

    def connect(self, input_ops, output_ops):
        """Connects `input_ops` and `output_ops` using data streams.

        Draws directed edges from `input_ops` to `ouput_ops` in the dataflow
        graph. `output_ops` can subscribe to any data streams on which any of
        of the `input_ops` publish.

        Args:
            input_ops (list of Op): Operators that publish on
        """
        # Check that all operators are children of the current graph
        for op_id in input_ops + output_ops:
            handle = self.op_handles[op_id]
            assert handle.graph_name == self.graph_name, \
                    'Can only connect operators for which graph {} is the parent'.format(self.graph_name)

        for op_id in input_ops:
            handle = self.op_handles[op_id]
            handle.dependant_ops += output_ops

    def construct(self, input_ops, **kwargs):
        """Constructs a graph

        Args:
            input_ops (list of Op): Operators to subscribe to.

        Returns:
            (list of Op): Output operators.
        """
        raise NotImplementedError("User must define setup_streams in graph.")

    @staticmethod
    def setup_streams(input_streams, **kwargs):
        """Subscribes to input data streams and constructs output data streams.

        Required user override.

        Args:
            input_streams (DataStreams): data streams from connected upstream
                operators which the operator may subscribe to.
            kwargs: Arbitrary keyword arguments used to subscribe to input
                data streams or construct output data streams.

        Returns:
            (list of DataStream): output data streams on which the operator
            publishes.
        """
        # TODO(peter): implement this after the stream redesign.
        raise NotImplementedError(
            "setups_streams will be exposed after API changes.")

    def execute(self, framework=None, blocking=True):
        """Execute the current graph.

        Args:
            framework (str): The name of the framework to use to execute the
                operators. Either ROS or Ray.
        """
        # 0. Setup subgraphs
        self._flatten_subgraphs()

        # 1. Build refined stream graph.
        self._build_refined_op_graph()

        # 2. Initiate backend framework.
        if framework:
            self.framework = framework
        self._init_frameworks()

        # 3. Set the execution framework on each operator handle.
        for op_id, op_handle in self.op_handles.items():
            op_handle.framework = self.framework

        # 4. Logging
        if FLAGS.log_graph:
            log_graph_to_dot_file('erdos.gv', self.op_handles.keys(),
                                  self._get_edges())

        # 5. Create executors in postorder.
        executors = self._create_executors()

        # 6. Setup the executors.
        for executor in executors:
            executor.setup()

        # 7. Construct the graph of dependent operator handles.
        dependent_op_handles = self._build_dependent_op_handles()

        # 8. Execute the graph
        for executor in executors:
            executor.op_handle._build_dependent_op_handles(
                dependent_op_handles)
            executor.execute()

        if blocking:
            # 9. Keep driver running.
            if self.framework == "ros":
                procs = list()
                for op_handle in self.op_handles.values():
                    procs.append(op_handle.executor_handle)
                for p in procs:
                    p.join()
            else:
                # TODO(yika): FIX! Temporary solution to keep Ray master running.
                while True:
                    time.sleep(5)

    def _flatten_subgraphs(self):
        """Set up subgraphs"""
        # TODO(peter) fix this after graphs implement setup_streams
        for graph_id, graph_handle in self.graph_handles.items():
            # Instantiate child graph
            subgraph = graph_handle.setup_graph()
            # Flatten grandchildren
            subgraph._flatten_subgraphs()
            # Add child op handles
            self.op_handles.update(subgraph.op_handles)
            # Point graph_id to child's input op
            self.op_handles[graph_id] = self.op_handles.pop(subgraph.input_op)

    def _build_refined_op_graph(self):
        """Refines the operator graph.

        Instantiates all data streams connecting operators. Repeatedly calls
        each operator's `setup_streams` method until the data streams
        converge.
        """
        not_converged = True
        while not_converged:
            not_converged = False
            # If outputs of setup_streams() change, update output_streams.
            for op_id, op_handle in self.op_handles.items():
                current_input_streams = op_handle.input_streams
                try:
                    output_streams = op_handle.op_cls.setup_streams(
                        DataStreams(current_input_streams),
                        **op_handle.setup_args)
                    for stream in output_streams:
                        stream.uid = op_id
                except TypeError as e:
                    if len(e.args) > 0 and e.args[0].startswith(
                            "setup_streams"):
                        setup_streams_name = "{0}.setup_streams".format(
                            op_handle.op_cls.__name__)
                        first_arg = "{0}.{1}".format(op_handle.op_cls.__name__,
                                                     e.args[0])
                        e.args = (first_arg, ) + e.args[1:]
                    raise
                # Check if the returned output_streams has changed.
                if self._different_output_streams(op_handle.output_streams,
                                                  output_streams):
                    not_converged = True
                # Update output streams to reset registered sinks. Otherwise,
                # we may end up with sinks that are added several times.
                op_handle.output_streams = output_streams

            # Empty input streams in order to ensure that we don't maintain
            # inputs streams that are removed between iterations.
            for op_id, op_handle in self.op_handles.items():
                # Deep copy input streams.
                op_handle.input_streams = op_handle.driver_input_streams[:]
            # We do this to break the reference cycle and ensure that the
            # output data stream object of the upstream operator is different
            # from the input data stream object of the downstream operator.
            for op_id, op_handle in self.op_handles.items():
                # Transform output streams into input streams.
                for dependant_id in op_handle.dependant_ops:
                    # Ensure that each operator receives a copy of the output
                    # streams as input streams. Otherwise, two operators that
                    # have the same output stream as input will work on a shared
                    # object. This object will contain the callbacks both
                    # operators register.
                    input_streams = [
                        out_stream._copy_stream()
                        for out_stream in op_handle.output_streams
                    ]
                    self.op_handles[
                        dependant_id].input_streams += input_streams

        # We must call setup_streams again after convergence to ensure that the
        # callbacks that are added in setup_streams are added on the latest copy
        # of the data streams.
        for op_id, op_handle in self.op_handles.items():
            op_handle.op_cls.setup_streams(
                DataStreams(op_handle.input_streams), **op_handle.setup_args)

        self._build_output_stream_sinks_graph()

    def _build_output_stream_sinks_graph(self):
        # Create sink graph using only op names
        # sink is the op that an output stream is flowing in
        # Because we have not initiated op or executor, we use op name
        for op_id, op_handle in self.op_handles.items():
            for stream in op_handle.output_streams:
                sinks = self.output_stream_to_op_id_sinks.get(stream.uid, [])
                sinks.append(op_id)
                self.output_stream_to_op_id_sinks[stream.uid] = sinks

    def _build_dependent_op_handles(self):
        dependent_op_handles = {}
        for op_id, op_handle in self.op_handles.items():
            for stream in op_handle.input_streams:
                exec_handles = dependent_op_handles.get(stream.uid, set([]))
                exec_handles.add(op_handle.executor_handle)
                dependent_op_handles[stream.uid] = exec_handles
        for stream_name, handles in dependent_op_handles.items():
            dependent_op_handles[stream_name] = list(handles)
        return dependent_op_handles

    def _different_output_streams(self, output_stream1, output_stream2):
        if len(output_stream1) != len(output_stream2):
            return True
        os_set1 = set([output_stream.uid for output_stream in output_stream1])
        os_set2 = set([output_stream.uid for output_stream in output_stream2])
        return len(os_set1.intersection(os_set2)) != len(os_set1)

    def _get_source_op_handles(self):
        src_op_handles = []
        for op_id, op_handle in self.op_handles.items():
            if len(op_handle.input_streams) < 1:
                src_op_handles.append(op_id)
        return src_op_handles

    def _get_edges(self):
        edges = []
        src_op_handles = self._get_source_op_handles()
        visited = set([])
        for op_id in src_op_handles:
            self._get_edges_helper(op_id, edges, visited)
        return edges

    def _get_edges_helper(self, op_id, edges, visited):
        visited.add(op_id)
        for dependant_op_id in self.op_handles[op_id].dependant_ops:
            # TODO(ionel): Get the stream to which the dependant operator
            # subscribers to so that the logged graph includes stream names
            # as well.
            edges.append((op_id, dependant_op_id, ""))
            if dependant_op_id not in visited:
                self._get_edges_helper(dependant_op_id, edges, visited)

    def _init_frameworks(self):
        """Initialize the frameworks."""
        if self.framework == "ros":
            import rosgraph
            if not rosgraph.is_master_online():
                # Run roscore in a different process
                subprocess.Popen("roscore")
                time.sleep(2)
        elif self.framework == "ray":
            self._init_ray()

    def _init_ray(self):
        import ray
        if FLAGS.ray_redis_address == '':
            ray.init()
        else:
            ray.init(
                redis_address=FLAGS.ray_redis_address)
            time.sleep(2)

    def _create_executors(self):
        visited = set([])
        executors = []
        src_op_handles = self._get_source_op_handles()
        for op_id in src_op_handles:
            # Traverse the execution graph in postorder so that receivers
            # are already up when senders are started.
            self._postorder_init_executors(op_id, visited, executors)
        for op_id in self.op_handles:
            if op_id not in visited:
                self._postorder_init_executors(op_id, visited, executors)
        return executors

    def _postorder_init_executors(self, op_id, visited, executors):
        """Traverse and initiate executors of the op_handles in postorder."""
        visited.add(op_id)
        for output_stream in self.op_handles[op_id].output_streams:
            for sink_op_id in self.output_stream_to_op_id_sinks.get(
                    output_stream, []):
                if sink_op_id not in visited:
                    self._postorder_init_executors(sink_op_id, visited,
                                                   executors)
        executor = self._create_executor(op_id)
        executors.append(executor)

    def _create_executor(self, op_id):
        op_handle = self.op_handles[op_id]
        if self.framework == 'ros':
            from erdos.ros.ros_executor import ROSExecutor
            ros_executor = ROSExecutor(op_handle)
            return ros_executor
        elif self.framework == 'ray':
            from erdos.ray.ray_executor import RayExecutor
            ray_executor = RayExecutor(op_handle)
            return ray_executor
        elif self.framework == 'local':
            return LocalExecutor(op_handle.name)
        else:
            raise Exception('Unexpected framework {}'.format(self.framework))


DEFAULT_GRAPH = Graph()


def get_current_graph():
    return DEFAULT_GRAPH
