import logging
import rosgraph
import rospy
from multiprocessing import Process
import cloudpickle
import pipes

from erdos.cluster.node import Node
from erdos.ros.ros_input_data_stream import ROSInputDataStream
from erdos.ros.ros_output_data_stream import ROSOutputDataStream

logger = logging.getLogger(__name__)


def _execute_operator(op_handle):
    proc = Process(target=lambda: _execute_helper(op_handle))
    proc.start()
    op_handle.executor_handle = proc

def _execute_helper(op_handle):
    op = _init_operator(op_handle)
    rospy.init_node(op.name, anonymous=True)
    op._internal_setup_streams()
    op.execute()

def _init_operator(op_handle):
    """
    Initiate op, set op streams, wrap op streams into framework
    """
    # Init op.
    try:
        op = op_handle.op_cls(op_handle.name,
                                   **op_handle.init_args)
        op.framework = op_handle.framework
    except TypeError as e:
        if len(e.args) > 0 and e.args[0].startswith("__init__"):
            first_arg = "{0}.{1}".format(op_handle.op_cls.__name__,
                                         e.args[0])
            e.args = (first_arg, ) + e.args[1:]
        raise

    # Set input/output streams
    ros_input_streams = [
        ROSInputDataStream(op, input_stream)
        for input_stream in op_handle.input_streams
    ]
    ros_output_streams = [
        ROSOutputDataStream(op, output_stream)
        for output_stream in op_handle.output_streams
    ]
    op._add_input_streams(ros_input_streams)
    op._add_output_streams(ros_output_streams)
    return op


class ROSNode(Node):
    def setup(self, ros_master_uri):
        ros_start_command = "ROS_MASTER_URI={} roscore".format(ros_master_uri)
        self.run_long_command_asnyc(ros_start_command)

    def teardown(self):
        self.run_command_sync("killall -9 roscore")

    def setup_operator(self, op_handle):
        pass

    def execute_operator(self, op_handle):
        data = cloudpickle.dumps(lambda : _execute_operator(op_handle))
        # TODO: run this on the remote node


class LocalROSNode(ROSNode):
    def __init__(self, resources=None):
        super(LocalROSNode, self).__init__("127.0.0.1", "", "", resources)

    def setup(self):
        if not rosgraph.is_master_online():
            # Run roscore in a different process
            subprocess.Popen("roscore")
            time.sleep(2)

    def execute_operator(self, op_handle):
        _execute_operator(op_handle)

    def teardown(self):
        rospy.signal_shutdown("Tearing down local node")

    def _make_dispatcher(self):
        return None
