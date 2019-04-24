import math
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_csv_logging, setup_logging, time_epoch_ms

import agent_utils
from planner.map import CarlaMap
from planner.waypointer import Waypointer
from pid_controller.pid import PID
import utils

class GroundAgentOperator(Op):
    def __init__(self,
                 name,
                 city_name,
                 goal_location,
                 goal_orientation,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(GroundAgentOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._map = CarlaMap(city_name)
        self._goal_location = goal_location
        self._goal_orientation = goal_orientation
        self._flags = flags
        self._wp_num_steer = 0.9  # Select WP - Reverse Order: 1 - closest, 0 - furthest
        self._wp_num_speed = 0.4  # Select WP - Reverse Order: 1 - closest, 0 - furthest
        self._waypointer = Waypointer(city_name)
        self._pid = PID(p=flags.pid_p, i=flags.pid_i, d=flags.pid_d)
        self._vehicle_pos = None
        self._vehicle_acc = None
        self._vehicle_speed = None
        self._pedestrians = []
        self._vehicles = []
        self._traffic_lights = []
        self._traffic_signs = []

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(utils.is_ground_vehicle_pos_stream).add_callback(
            GroundAgentOperator.on_vehicle_pos_update)
        input_streams.filter(utils.is_ground_acceleration_stream).add_callback(
            GroundAgentOperator.on_vehicle_acceleration_update)
        input_streams.filter(utils.is_ground_forward_speed_stream).add_callback(
            GroundAgentOperator.on_forward_speed_update)
        input_streams.filter(utils.is_ground_pedestrians_stream).add_callback(
            GroundAgentOperator.on_pedestrians_update)
        input_streams.filter(utils.is_ground_vehicles_stream).add_callback(
            GroundAgentOperator.on_vehicles_update)
        input_streams.filter(utils.is_ground_traffic_lights_stream).add_callback(
            GroundAgentOperator.on_traffic_lights_update)
        input_streams.filter(utils.is_ground_traffic_signs_stream).add_callback(
            GroundAgentOperator.on_traffic_signs_update)
        # Set no watermark on the output stream so that we do not
        # close the watermark loop with the carla operator.
        return [utils.create_agent_action_stream()]

    def on_vehicle_pos_update(self, msg):
        self._logger.info("Received vehicle pos %s", msg)
        self._vehicle_pos = msg.data

    def on_vehicle_acceleration_update(self, msg):
        self._vehicle_acc = msg.data

    def on_forward_speed_update(self, msg):
        self._vehicle_speed = msg.data

    def on_pedestrians_update(self, msg):
        self._pedestrians = msg.data

    def on_vehicles_update(self, msg):
        self._vehicles = msg.data

    def on_traffic_lights_update(self, msg):
        self._traffic_lights = msg.data

    def on_traffic_signs_update(self, msg):
        self._traffic_signs = msg.data

    # TODO(ionel): Set the frequency programmatically.
    @frequency(10)
    def run_step(self):
        self._logger.info("Running step")
        if (self._vehicle_pos is None or self._vehicle_acc is None
                or self._vehicle_speed is None or self._pedestrians == []
                or self._vehicles == [] or self._traffic_lights == []):
            return

        start_time = time.time()

        wp_angle, wp_vector, wp_angle_speed, wp_vector_speed = agent_utils.get_waypoints(
            self._goal_location, self._goal_orientation, self._vehicle_pos, self._waypointer,
            self._wp_num_steer, self._wp_num_speed)

        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},{}'.format(
            time_epoch_ms(), self.name, runtime))

        speed_factor, state = self.stop_for_agents(
            wp_angle, wp_vector, self._vehicles, self._pedestrians,
            self._traffic_lights)
        control = self.get_control(wp_angle, wp_angle_speed, speed_factor,
                                   self._vehicle_speed * 3.6)
        output_msg = Message(control, Timestamp(coordinates=[0]))
        self.get_output_stream('action_stream').send(output_msg)

    def stop_for_agents(self, wp_angle, wp_vector, vehicles, pedestrians,
                        traffic_lights):
        speed_factor = 1
        speed_factor_tl = 1
        speed_factor_p = 1
        speed_factor_v = 1

        if self._flags.stop_for_vehicles:
            for vehicle in vehicles:
                obs_vehicle_pos = (vehicle.position.location[0],
                                   vehicle.position.location[1])
                if agent_utils.is_vehicle_on_same_lane(
                        self._vehicle_pos, obs_vehicle_pos, self._map):
                    new_speed_factor_v = agent_utils.stop_vehicle(
                        self._vehicle_pos,
                        obs_vehicle_pos,
                        wp_vector,
                        speed_factor_v,
                        self._flags)
                    speed_factor_v = min(speed_factor_v, new_speed_factor_v)

        if self._flags.stop_for_pedestrians:
            for pedestrian in pedestrians:
                ped_pos = (pedestrian.position.location[0],
                           pedestrian.position.location[1])
                if agent_utils.is_pedestrian_hitable(ped_pos, self._map):
                    new_speed_factor_p = agent_utils.stop_pedestrian(
                        self._vehicle_pos,
                        ped_pos,
                        wp_vector,
                        speed_factor_p,
                        self._flags)
                    speed_factor_p = min(speed_factor_p, new_speed_factor_p)

        if self._flags.stop_for_traffic_lights:
            for tl in traffic_lights:
                tl_pos = (tl.position.location[0], tl.position.location[1])
                if (agent_utils.is_traffic_light_active(
                        self._vehicle_pos, tl_pos, self._map) and
                    agent_utils.is_traffic_light_visible(
                        self._vehicle_pos, tl_pos, self._flags)):
                    new_speed_factor_tl = agent_utils.stop_traffic_light(
                        self._vehicle_pos,
                        tl_pos,
                        tl.state,
                        wp_vector,
                        wp_angle,
                        speed_factor_tl,
                        self._flags)
                    speed_factor_tl = min(speed_factor_tl, new_speed_factor_tl)

        speed_factor = min(speed_factor_tl, speed_factor_p, speed_factor_v)
        state = {
            'stop_pedestrian': speed_factor_p,
            'stop_vehicle': speed_factor_v,
            'stop_traffic_lights': speed_factor_tl
        }

        return speed_factor, state

    def execute(self):
        self.run_step()
        self.spin()

    def get_control(self, wp_angle, wp_angle_speed, speed_factor,
                    current_speed):
        current_speed = max(current_speed, 0)
        steer = self._flags.steer_gain * wp_angle
        if steer > 0:
            steer = min(steer, 1)
        else:
            steer = max(steer, -1)

        # Don't go to fast around corners
        if math.fabs(wp_angle_speed) < 0.1:
            target_speed_adjusted = self._flags.target_speed * speed_factor
        elif math.fabs(wp_angle_speed) < 0.5:
            target_speed_adjusted = 20 * speed_factor
        else:
            target_speed_adjusted = 15 * speed_factor

        self._pid.target = target_speed_adjusted
        pid_gain = self._pid(feedback=current_speed)
        throttle = min(
            max(self._flags.default_throttle - 1.3 * pid_gain, 0),
            self._flags.throttle_max)

        if pid_gain > 0.5:
            brake = min(0.35 * pid_gain * self._flags.brake_strength, 1)
        else:
            brake = 0

        return {
            'steer': steer,
            'throttle': throttle,
            'brake': brake,
            'hand_brake': False,
            'reverse': False
        }
