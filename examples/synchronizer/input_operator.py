import os
import random
import sys
import time

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp

from std_msgs.msg import String


class InputOperator(Op):
    def __init__(self, name='input'):
        super(InputOperator, self).__init__(name)
        self._epoch = 0

    @staticmethod
    def setup_streams(input_streams, op_name):
        return [DataStream(data_type=String, name='{}_output'.format(op_name))]

    def publish_inputs(self):
        while True:
            msg_delay_time = random.random()
            time.sleep(msg_delay_time)
            # data = 'message {} from {}'.format(self._epoch, self.name)
            data = time.time() * 1000.0
            msg = Message(data, Timestamp(coordinates=[self._epoch]))
            print('Publish {}'.format(msg))
            self.get_output_stream('{}_output'.format(self.name)).send(msg)
            time.sleep(1.0 - msg_delay_time)
            self._epoch += 1

    def execute(self):
        self.publish_inputs()
