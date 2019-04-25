import logging
import rosgraph

from erdos.ray.ray_operator import RosOperator
from erdos.cluster.node import Node

logger = logging.getLogger(__name__)


class RosNode(Node):
    def setup(self):
        raise NotImplementedError

    def teardown(self):
        raise NotImplementedError

    def setup_operator(self, op_handle):
        pass

    def execute_operator(self, op_handle):
        raise NotImplementedError


class LocalRosNode(RosNode):
    def __init__(self, resources=None):
        super(LocalRosNode, self).__init__("127.0.0.1", "", "", resources)

    def setup(self):
        if not rosgraph.is_master_online():
            # Run roscore in a different process
            subprocess.Popen("roscore")
            time.sleep(2)

    def execute(self):
        proc = Process(target=self._execute_helper)
        proc.start()
        self.op_handle.executor_handle = proc

    def _execute_helper(self):
        op = self._init_operator()
        rospy.init_node(op.name, anonymous=True)
        op._internal_setup_streams()
        op.execute()

    def _init_operator(self):
        """
        Initiate op, set op streams, wrap op streams into framework
        """
        # Init op.
        try:
            op = self.op_handle.op_cls(self.op_handle.name,
                                       **self.op_handle.init_args)
            op.framework = self.op_handle.framework
        except TypeError as e:
            if len(e.args) > 0 and e.args[0].startswith("__init__"):
                first_arg = "{0}.{1}".format(self.op_handle.op_cls.__name__,
                                             e.args[0])
                e.args = (first_arg, ) + e.args[1:]
            raise

        # Set input/output streams
        ros_input_streams = [
            ROSInputDataStream(op, input_stream)
            for input_stream in self.op_handle.input_streams
        ]
        ros_output_streams = [
            ROSOutputDataStream(op, output_stream)
            for output_stream in self.op_handle.output_streams
        ]
        op._add_input_streams(ros_input_streams)
        op._add_output_streams(ros_output_streams)
        return op

    def teardown(self):
        rospy.signal_shutdown("Tearing down local node")

    def _make_dispatcher(self):
        return None
