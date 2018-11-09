from multiprocessing import Process
import rospy

from erdos.executor import Executor
from erdos.ros.ros_input_data_stream import ROSInputDataStream
from erdos.ros.ros_output_data_stream import ROSOutputDataStream


class ROSExecutor(Executor):
    """Class wrapping the common logic for starting ROS operators.

       Attributes:
           op: The operator to be started.
    """

    def __init__(self, op_handle):
        super(ROSExecutor, self).__init__(op_handle)

    def execute(self):
        """Executing a ROS node."""
        # Start the operator in a new thread.
        proc = Process(target=self._execute_helper)
        # TODO(ionel): ROS nodes cannot execute on other machines. Fix!
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
            ROSOutputDataStream(output_stream)
            for output_stream in self.op_handle.output_streams
        ]
        op._add_input_streams(ros_input_streams)
        op._add_output_streams(ros_output_streams)
        return op
