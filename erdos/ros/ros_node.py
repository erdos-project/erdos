import logging
import ray
import rosgraph
import rospy
from multiprocessing import Process
import subprocess
import sys
import time

if sys.version_info > (3, 0):
    from urllib.parse import urlparse
else:
    from urlparse import urlparse

from erdos.ros.ros_input_data_stream import ROSInputDataStream
from erdos.ros.ros_output_data_stream import ROSOutputDataStream
from erdos.ray.ray_node import RayNode, LocalRayNode

logger = logging.getLogger(__name__)


@ray.remote
class RemoteROSExecutor(object):
    """Executes and manages processes on a remote node"""

    def __init__(self):
        self.processes = []

    def teardown(self):
        for process in self.processes:
            process.terminate()
        rospy.signal_shutdown("Tearing down local node")

    def execute_operator(self, op_handle):
        proc = Process(target=lambda: self._execute_helper(op_handle))
        proc.start()
        op_handle.executor_handle = proc
        self.processes.append(proc)

    def _execute_helper(self, op_handle):
        op = self._init_operator(op_handle)
        rospy.init_node(op.name, anonymous=True)
        op._internal_setup_streams()
        op.execute()

    def _init_operator(self, op_handle):
        """
        Initiate op, set op streams, wrap op streams into framework
        """
        # Init op.
        try:
            op = op_handle.op_cls(op_handle.name, **op_handle.init_args)
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


class ROSNode(RayNode):
    def __init__(self, server, username, ssh_key, resources=None):
        # Note: this should be identical to LocalROSNode.__init__
        super(ROSNode, self).__init__(server, username, ssh_key, resources)
        self.executor = None

    def setup(self, redis_address, ros_master_uri):
        super(ROSNode, self).setup(redis_address)

        ros_start_command = "ROS_MASTER_URI={} roscore".format(ros_master_uri)
        self.run_long_command_asnyc(ros_start_command)

        self.executor = RemoteROSExecutor._remote(
            num_cpus=0, resources={self.server: 1})

    def teardown(self):
        super(ROSNode, self).teardown()

        self.executor.teardown.remote()
        self.run_command_sync("killall -9 roscore")

    def setup_operator(self, op_handle):
        # Note: this should be identical to LocalROSNode.setup_operator
        self.op_handles.append(op_handle)
        op_handle.node = None

    def execute_operator(self, op_handle):
        # Note: this should be identical to LocalROSNode.execute_operator
        assert op_handle in self.op_handles
        self.executor.execute_operator.remote(op_handle)


class LocalROSNode(LocalRayNode):
    def __init__(self, resources=None):
        # Note: this should be identical to ROSNode.__init__
        super(LocalROSNode, self).__init__(resources)
        self.executor = None

    def setup(self):
        redis_address = super(LocalROSNode, self).setup()
        if not rosgraph.is_master_online():
            # Run roscore in a different process
            subprocess.Popen("roscore")
            time.sleep(2)

        self.executor = RemoteROSExecutor._remote(
            num_cpus=0, resources={self.server: 1})
        master = rosgraph.Master("/mynode")
        parsed_ros_uri = urlparse(master.lookupNode("/rosout"))
        master_ip = ray.services.get_node_ip_address()
        ros_uri = "{}://{}:{}/".format(parsed_ros_uri.scheme, master_ip,
                                       parsed_ros_uri.port)

        return redis_address, ros_uri

    def teardown(self):
        super(LocalROSNode, self).teardown()

        self.executor.teardown.remote()

    def setup_operator(self, op_handle):
        # Note: this should be identical to ROSNode.setup_operator
        self.op_handles.append(op_handle)
        op_handle.node = None

    def execute_operator(self, op_handle):
        # Note: this should be identical to ROSNode.execute_operator
        assert op_handle in self.op_handles
        self.executor.execute_operator.remote(op_handle)

    def _make_dispatcher(self):
        return None
