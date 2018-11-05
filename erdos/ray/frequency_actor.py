from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

import ray


@ray.remote
class FrequencyActor(object):
    def __init__(self, ray_op):
        self.ray_op = ray_op

    def set_frequency(self, rate, func_name, *args):
        period = 1 / rate
        trigger_at = time.time() + period
        while True:
            time_until_trigger = trigger_at - time.time()
            if time_until_trigger > 0:
                time.sleep(time_until_trigger)
            else:
                logging.warning('Cannot run {} at desired rate {}'.format(
                    func_name, rate))
            self.ray_op.on_frequency.remote(func_name, *args)
            trigger_at += period
