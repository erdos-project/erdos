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
                 pre_failure_time_elapse_s=None,
                 failure_duration_s=None,
                 log_file_name=None):
        super(ControllerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._pre_failure_time_elapse_s = pre_failure_time_elapse_s   # seconds elapsed before failure trigger
        self._failure_duration_s = failure_duration_s    # seconds elapsed after failure until recovery

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(name='controller_stream',
                           labels={'control_stream': 'true'})]

    def execute(self):
        # TODO(ionel): We should send the failure message on a failed stream,
        # and get a reply back so that we know the node is not processing or
        # issuing any new messages. Otherwise, we run into the risk of having
        # inconsistent state because we can't guarantees in-order delivery
        # across operators.
        
        # Send failure message.
        if self._pre_failure_time_elapse_s is not None:
            time.sleep(self._pre_failure_time_elapse_s)
            # Fail primary and notify ingress, egress and consumers
            fail_replica_num = 0
            fail_msg = Message((flux_utils.FluxControllerCommand.FAIL, fail_replica_num), Timestamp(coordinates=[0]))
            self.get_output_stream('controller_stream').send(fail_msg)
            print("Control send failure message to primary")

            if self._failure_duration_s is not None:
                # TODO(yika): recovery does not work yet
                # Recover failed replica
                time.sleep(self._failure_duration_s)
                fail_msg = Message((flux_utils.FluxControllerCommand.RECOVER, fail_replica_num), Timestamp(coordinates=[0]))
                self.get_output_stream('controller_stream').send(fail_msg)

        self.spin()
