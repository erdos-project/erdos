import logging
import rospy

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op


class ROSServiceOp(Op):
    """Bridges between ROS services and ERDOS streams.

    Attributes:
        service_name: Name of the service.
        srv_type: Type of the service request.
        srv_ret_type: Type of the service response.
    """

    def __init__(self, name, service_name, srv_type):
        super(ROSServiceOp, self).__init__(name)
        self._service_name = service_name
        rospy.wait_for_service(service_name)
        try:
            self._srv_proxy = rospy.ServiceProxy(service_name, srv_type)
        except rospy.ServiceException as expt:
            logging.error('Could not create service proxy %s', expt)

    @staticmethod
    def setup_streams(input_streams, srv_ret_type, op_name):
        """Subscribes to input data streams and publishes the service response.

        Args:
            input_streams (DataStreams): Data streams to which the operator
                subscribes. The operator sends messages from these streams to
                the service as requests.
            srv_ret_type: Type of the service response.
            op_name (str): Operator name used to name the output data stream.

        Returns:
            (list of DataStream): 1 data stream sending service responses.
        """

        input_streams.add_callback(ROSServiceOp.on_call)
        return [
            DataStream(
                data_type=srv_ret_type, name='{}_output'.format(op_name))
        ]

    def call(self, *args):
        try:
            ret = self._srv_proxy(*args)
            return ret
        except rospy.ServiceException as expt:
            logging.error('Service call failed %s', expt)

    def on_call(self, msg):
        reply = self.call(msg)
        self.get_output_stream(self.name).send(Message(reply, msg.timestamp))

    def execute(self):
        if self.framework != 'ros':
            rospy.init_node(self._service_name, anonymous=True)
        self.spin()
