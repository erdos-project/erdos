import time

from erdos.message import Message
from erdos.op import Op
from erdos.utils import frequency, setup_csv_logging, setup_logging, time_epoch_ms

from control.agent_utils import get_angle, get_world_vec_dist
from simulation.planner.waypointer import Waypointer
import pylot_utils


class WaypointerOperator(Op):
    def __init__(self,
                 name,
                 city_name,
                 goal_location,
                 goal_orientation,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(WaypointerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._goal_location = goal_location
        self._goal_orientation = goal_orientation
        self._waypointer = Waypointer(city_name)
        self._wp_num_steer = 0.9  # Select WP - Reverse Order: 1 - closest, 0 - furthest
        self._wp_num_speed = 0.4  # Select WP - Reverse Order: 1 - closest, 0 - furthest

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(pylot_utils.is_ground_vehicle_pos_stream).add_callback(
            WaypointerOperator.on_vehicle_pos_update)
        return [pylot_utils.create_waypoints_stream()]

    def on_vehicle_pos_update(self, msg):
        start_time = time.time()
        payload = self.get_waypoints(msg.data)
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},{}'.format(
            time_epoch_ms(), self.name, runtime))
        self.get_output_stream('waypoints').send(
            Message(payload, msg.timestamp))

    def get_waypoints(self, vehicle_pos):
        vehicle_x = vehicle_pos[0][0]
        vehicle_y = vehicle_pos[0][1]
        vehicle_ori_x = vehicle_pos[1][0]
        vehicle_ori_y = vehicle_pos[1][1]
        vehicle_ori_z = vehicle_pos[1][2]

        waypoints_world, waypoints, route = self._waypointer.get_next_waypoints(
            (vehicle_x, vehicle_y, 0.22),
            (vehicle_ori_x, vehicle_ori_y, vehicle_ori_z),
            self._goal_location,
            self._goal_orientation)

        if waypoints_world == []:
            waypoints_world = [[vehicle_x, vehicle_y, 0.22]]

        # Make a function, maybe util function to get the magnitues
        wp = [
            waypoints_world[int(self._wp_num_steer * len(waypoints_world))][0],
            waypoints_world[int(self._wp_num_steer * len(waypoints_world))][1]
        ]

        wp_vector, wp_mag = get_world_vec_dist(wp[0], wp[1], vehicle_x, vehicle_y)

        if wp_mag > 0:
            wp_angle = get_angle(wp_vector, [vehicle_ori_x, vehicle_ori_y])
        else:
            wp_angle = 0

        wp_speed = [
            waypoints_world[int(self._wp_num_speed * len(waypoints_world))][0],
            waypoints_world[int(self._wp_num_speed * len(waypoints_world))][1]
        ]

        wp_vector_speed, _ = get_world_vec_dist(
            wp_speed[0], wp_speed[1], vehicle_x, vehicle_y)

        wp_angle_speed = get_angle(
            wp_vector_speed, [vehicle_ori_x, vehicle_ori_y])

        return (wp_angle, wp_vector, wp_angle_speed, wp_vector_speed)
