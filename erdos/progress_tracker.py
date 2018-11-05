from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


@ray.remote
class ProgressTracker(object):
    def __init__(self):
        self._notif_time_endpoints = {}
        self._endpoints = {}
        self._completed_times = {}

    def register_endpoint(self, endpoint_name, notif_actor):
        self._endpoints[endpoint_name] = notif_actor

    def notify_at(self, endpoint_name, time):
        """Called when an endpoint registers for a notification."""
        if time not in self._notif_time_endpoints:
            self._notif_time_endpoints[time] = [endpoint_name]
        else:
            self._notif_time_endpoints[time].append(enpoint)

    def complete_time(self, endpoint_name, time):
        """Called when an endpoint completes a time."""
        if time not in self._completed_times:
            self._completed_times[time] = 1
        else:
            self._completed_times[time] += 1

        if self._completed_times[time]:
            self.notify(time)

    def notify(self, time):
        """Send notifications to endpoints."""
        for endpoint_name in self._notif_time_endpoints[time]:
            self._endpoint[endpoint_name].on_notify.remote(time)
        del self._notif_time_endpoints[time]
