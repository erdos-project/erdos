import math
try:
    import queue as queue
except ImportError:
    import Queue as queue
import carla

from erdos.op import Op
from erdos.utils import frequency, setup_csv_logging, setup_logging

import pylot_utils
from planning.messages import WaypointMessage


def get_distance(loc1, loc2):
    x_diff = loc1.x - loc2.x
    y_diff = loc1.y - loc2.y
    return math.sqrt(x_diff**2 + y_diff**2)


class PlanningOperator(Op):
    def __init__(self, name, flags, log_file_name=None, csv_file_name=None):
        super(PlanningOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._flags = flags
        self._sampling_resolution = 1
        self._min_distance = self._sampling_resolution * 0.9
        self._map = None
        self._waypoints = None
        self._vehicle_transform = None
        self._last_waypoint = None
        self._last_road_option = None

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(pylot_utils.is_ground_vehicle_transform_stream).add_callback(
            PlanningOperator.on_vehicle_transform_update)
        input_streams.filter(pylot_utils.is_open_drive_stream).add_callback(
            PlanningOperator.on_opendrive_map)
        input_streams.filter(pylot_utils.is_global_trajectory_stream).add_callback(
            PlanningOperator.on_global_trajectory)
        return [pylot_utils.create_waypoints_stream()]

    def on_vehicle_transform_update(self, msg):
        self._vehicle_transform = msg.data
        next_waypoint, road_option = self.__compute_next_waypoint()
        target_speed = 0
        if next_waypoint:
            target_speed = self.__local_control_decision(next_waypoint)
            self.get_output_stream('waypoints').send(
                WaypointMessage(msg.timestamp,
                                waypoint=next_waypoint,
                                target_speed=target_speed))

    def on_opendrive_map(self, msg):
        assert self._map is None, 'Already receveid opendrive map'
        self._map = carla.Map('test', msg.data)

    def on_global_trajectory(self, msg):
        assert self._waypoints is None, 'Already received global trajectory'
        self._waypoints = queue.Queue()
        for waypoint_option in msg.data:
            self._waypoints.put(waypoint_option)

    def __compute_next_waypoint(self):
        if self._waypoints is None:
            return None, None
        if self._waypoints.empty():
            self._logger.info('Reached end of waypoints')
            return None, None
        if self._last_waypoint is None:
            # If there was no last waypoint, pop one from the queue.
            next_waypoint, next_road_option = self._waypoints.get()
        else:
            # Find the next waypoint which is more than 90% left to complete.
            next_waypoint = self._last_waypoint
            next_road_option = self._last_road_option
            while (self._waypoints.empty() is False and
                   get_distance(
                       next_waypoint.location,
                       self._vehicle_transform.location) < self._min_distance):
                next_waypoint, next_road_option = self._waypoints.get()

        # If the next waypoint is different from the last waypoint, send
        # the next waypoint
        if next_waypoint != self._last_waypoint:
            self._last_waypoint = next_waypoint
            self._last_road_option = next_road_option
            self._logger.info('New waypoint {} {}'.format(
                self._last_waypoint, self._last_road_option))
            return next_waypoint, next_road_option
        else:
            self._logger.info('No new waypoint')
            return None, None


    def __local_control_decision(self, waypoint):
        if get_distance(waypoint.location,
                        self._vehicle_transform.location) > 0.08:
            target_speed = 10
        else:
            # We are reaching a waypoint reduce the speed to 0.
            target_speed = 0
        return target_speed
