from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


@ray.remote
class NotificationActor(object):
    def __init__(self):
        self._op = None

    def set_operator(self, op):
        self._op = op

    def on_notify(self, timestamp):
        self._op.on_notify(timestamp)
