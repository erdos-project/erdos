import math
import numpy as np
import time

from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_csv_logging, setup_logging, time_epoch_ms

from control.messages import ControlMessage
import control.utils as agent_utils
from pid_controller.pid import PID
from simulation.utils import get_3d_world_position
from simulation.planner.map import CarlaMap
import pylot_utils


class ERDOSAgentOperator(Op):
    def __init__(self,
                 name,
                 city_name,
                 depth_camera_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(ERDOSAgentOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._map = CarlaMap(city_name)
        self._pid = PID(p=self._flags.pid_p,
                        i=self._flags.pid_i,
                        d=self._flags.pid_d)
        self._world_transform = []
        self._depth_msgs = []
        self._traffic_lights = []
        self._obstacles = []
        self._vehicle_pos = None
        self._vehicle_acc = None
        self._vehicle_speed = None
        self._wp_angle = None
        self._wp_vector = None
        self._wp_angle_speed = None

    @staticmethod
    def setup_streams(input_streams, depth_camera_name):
        input_streams.filter_name(depth_camera_name).add_callback(
            ERDOSAgentOperator.on_depth_camera_update)

        # XXX(ionel): We get the exact position from the simulator.
        input_streams.filter(pylot_utils.is_world_transform_stream).add_callback(
            ERDOSAgentOperator.on_world_transform_update)
        input_streams.filter(pylot_utils.is_ground_vehicle_pos_stream).add_callback(
            ERDOSAgentOperator.on_vehicle_pos_update)
        input_streams.filter(pylot_utils.is_ground_acceleration_stream).add_callback(
            ERDOSAgentOperator.on_vehicle_acceleration_update)
        input_streams.filter(pylot_utils.is_ground_forward_speed_stream).add_callback(
            ERDOSAgentOperator.on_forward_speed_update)

        input_streams.filter(pylot_utils.is_waypoints_stream).add_callback(
            ERDOSAgentOperator.on_waypoints_update)
        input_streams.filter(pylot_utils.is_traffic_lights_stream).add_callback(
            ERDOSAgentOperator.on_traffic_lights_update)
        input_streams.filter(pylot_utils.is_segmented_camera_stream).add_callback(
            ERDOSAgentOperator.on_segmented_frame)
        input_streams.filter(pylot_utils.is_obstacles_stream).add_callback(
            ERDOSAgentOperator.on_obstacles_update)
        input_streams.filter(pylot_utils.is_detected_lane_stream).add_callback(
            ERDOSAgentOperator.on_detected_lane_update)

        input_streams.add_completion_callback(
            ERDOSAgentOperator.on_notification)

        # Set no watermark on the output stream so that we do not
        # close the watermark loop with the carla operator.
        return [pylot_utils.create_agent_action_stream()]

    def on_notification(self, msg):
        self._logger.info("Timestamps {} {} {} {}".format(
            self._obstacles[0].timestamp,
            self._traffic_lights[0].timestamp,
            self._depth_msgs[0].timestamp,
            self._world_transform[0].timestamp))
        world_transform = self._world_transform[0].data
        self._world_transform = self._world_transform[1:]

        depth_msg = self._depth_msgs[0]
        self._depth_msgs = self._depth_msgs[1:]

        traffic_lights = self.__transform_tl_output(depth_msg, world_transform)
        self._traffic_lights = self._traffic_lights[1:]

        (pedestrians, vehicles) = self.__transform_detector_output(
            depth_msg, world_transform)
        self._obstacles = self._obstacles[1:]

        speed_factor, state = self.__stop_for_agents(
            self._wp_angle, self._wp_vector, vehicles, pedestrians, traffic_lights)

        control_msg = self.get_control_message(
            self._wp_angle, self._wp_angle_speed, speed_factor,
            self._vehicle_speed * 3.6, Timestamp(coordinates=[0]))
        self.get_output_stream('action_stream').send(control_msg)

    def __is_ready_to_run(self):
        vehicle_data = self._vehicle_pos and self._vehicle_speed and self._wp_angle
        perception_data = (len(self._obstacles) > 0) and (len(self._traffic_lights) > 0)
        ground_data = (len(self._depth_msgs) > 0) and (len(self._world_transform) > 0)
        return vehicle_data and perception_data and ground_data

    def on_waypoints_update(self, msg):
        self._wp_angle = msg.wp_angle
        self._wp_vector = msg.wp_vector
        self._wp_angle_speed = msg.wp_angle_speed

    def on_world_transform_update(self, msg):
        self._world_transform.append(msg)

    def on_depth_camera_update(self, msg):
        self._depth_msgs.append(msg)

    def on_segmented_frame(self, msg):
        self._logger.info("Received segmented frame update at {}".format(msg.timestamp))
        # TODO(ionel): Implement!

    def on_traffic_lights_update(self, msg):
        self._logger.info("Received traffic light update at {}".format(msg.timestamp))
        self._traffic_lights.append(msg)

    def on_traffic_signs_update(self, msg):
        # TODO(ionel): Implement!
        pass

    def on_obstacles_update(self, msg):
        self._logger.info("Received obstacle update at {}".format(msg.timestamp))
        self._obstacles.append(msg)

    def on_detected_lane_update(self, msg):
        # TODO(ionel): Implement!
        pass

    def on_vehicle_pos_update(self, msg):
        self._logger.info("Received vehicle pos %s", msg)
        self._vehicle_pos = msg.data

    def on_vehicle_acceleration_update(self, msg):
        self._vehicle_acc = msg.data

    def on_forward_speed_update(self, msg):
        self._vehicle_speed = msg.data

    def execute(self):
        self.spin()

    def __transform_tl_output(self, depth_msg, world_transform):
        traffic_lights = []
        for tl in self._traffic_lights[0].detected_objects:
            x = (tl.corners[0] + tl.corners[1]) / 2
            y = (tl.corners[2] + tl.corners[3]) / 2
            (x3d, y3d, z3d) = get_3d_world_position(
                x, y, depth_msg, world_transform)
            state = 0
            if tl.label is not 'Green':
                state = 1
            traffic_lights.append((x3d, y3d, state))
        return traffic_lights

    def __transform_detector_output(self, depth_msg, world_transform):
        vehicles = []
        pedestrians = []
        for detected_obj in self._obstacles[0].detected_objects:
            x = (detected_obj.corners[0] + detected_obj.corners[1]) / 2
            y = (detected_obj.corners[2] + detected_obj.corners[3]) / 2
            if detected_obj.label == 'person':
                (x3d, y3d, z3d) = get_3d_world_position(
                    x, y, depth_msg, world_transform)
                pedestrians.append((x3d, y3d))
            elif (detected_obj.label == 'car' or
                  detected_obj.label == 'bicycle' or
                  detected_obj.label == 'motorcycle' or
                  detected_obj.label == 'bus' or
                  detected_obj.label == 'truck'):
                (x3d, y3d, z3d) = get_3d_world_position(
                    x, y, depth_msg, world_transform)
                vehicles.append((x3d, y3d))
        return (pedestrians, vehicles)

    def __stop_for_agents(
            self, wp_angle, wp_vector, vehicles, pedestrians, traffic_lights):
        speed_factor = 1
        speed_factor_tl = 1
        speed_factor_p = 1
        speed_factor_v = 1

        for vehicle in vehicles:
            if agent_utils.is_vehicle_on_same_lane(self._vehicle_pos, vehicle, self._map):
                new_speed_factor_v = agent_utils.stop_vehicle(
                    self._vehicle_pos, vehicle, wp_vector, speed_factor_v, self._flags)
                speed_factor_v = min(speed_factor_v, new_speed_factor_v)

        for pedestrian in pedestrians:
            if agent_utils.is_pedestrian_hitable(pedestrian, self._map):
                new_speed_factor_p = agent_utils.stop_pedestrian(
                    self._vehicle_pos,
                    pedestrian,
                    wp_vector,
                    speed_factor_p,
                    self._flags)
                speed_factor_p = min(speed_factor_p, new_speed_factor_p)

        for tl in traffic_lights:
            if (agent_utils.is_traffic_light_active(self._vehicle_pos, tl, self._map) and
                agent_utils.is_traffic_light_visible(self._vehicle_pos, tl, self._flags)):
                tl_state = tl[2]
                new_speed_factor_tl = agent_utils.stop_traffic_light(
                    self._vehicle_pos,
                    tl,
                    tl_state,
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
        self._logger.info('Aggent speed factors {}'.format(state))
        return speed_factor, state

    def get_control_message(self, wp_angle, wp_angle_speed, speed_factor,
                            current_speed, timestamp):
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

        return ControlMessage(steer, throttle, brake, False, False, timestamp)
