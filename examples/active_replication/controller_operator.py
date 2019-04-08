import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_logging

import flux_utils


class ControllerOperator(Op):
    def __init__(self,
                 name,
                 fail_timeout=1,
                 log_file_name=None):
        super(ControllerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._node_failed = False
        self._timeout = fail_timeout

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(ControllerOperator.on_failure_msg)
        return [DataStream(name='controller_stream',
                           labels={'control_stream': 'true'})]

    # XXX(ionel): This method is not currently invoked.
    def on_failure_msg(self, msg):
        assert self._node_failed is False
        self._node_failed = True
        # TODO(ionel): Investigate if the code works when we fail the primary.
        # Notify the nodes that a node has failed.
        fail_msg = Message(flux_utils.FAILED_REPLICA,
                           Timestamp(coordinates=[0]))
        self.get_output_stream('controller_stream').send(fail_msg)

    def execute(self):
        # TODO(ionel): We should send the failure message on a failed stream,
        # and get a reply back so that we know the node is not processing or
        # issuing any new messages. Otherwise, we run into the risk of having
        # inconsistent state because we can't guarantees in-order delivery
        # across operators.
        
        # Send failure message.
        time.sleep(3)
        # Fail primary and notify ingress, egress and consumers
        fail_replica_num = 0
        fail_msg = Message(fail_replica_num, Timestamp(coordinates=[0]))
        self.get_output_stream('controller_stream').send(fail_msg)
        print("Control send failure message to primary")

        # Recover failed primary
        # time.sleep(self._timeout)
        # fail_msg = Message(0, Timestamp(coordinates=[0]))
        # self.get_output_stream('controller_stream').send(fail_msg)

        self.spin()
