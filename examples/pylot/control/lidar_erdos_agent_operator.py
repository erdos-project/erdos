from collections import deque
import math

from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_csv_logging, setup_logging

from control.messages import ControlMessage
import control.utils as agent_utils
from pid_controller.pid import PID
import simulation.utils
from simulation.utils import get_3d_world_position
import pylot_utils


class ERDOSAgentOperator(Op):
    def __init__(self,
                 name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(ERDOSAgentOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._pid = PID(p=self._flags.pid_p,
                        i=self._flags.pid_i,
                        d=self._flags.pid_d)
        self._vehicle_transforms = deque()
        self._traffic_lights = []
        self._obstacles = []
        self._point_clouds = []
        self._vehicle_speed = None
        self._wp_angle = None
        self._wp_vector = None
        self._wp_angle_speed = None
        # TODO(ionel): DANGEROUS! DO NOT HARDCODE!
        loc = simulation.utils.Location(2.0, 0.0, 1.40)
        self._camera_transform = simulation.utils.Transform(
            loc, pitch=0, yaw=0, roll=0)
        self._camera_width = 800
        self._camera_height = 600
        self._camera_fov = 100


    @staticmethod
    def setup_streams(input_streams, depth_camera_name):
        input_streams.filter(pylot_utils.is_can_bus_stream).add_callback(
            ERDOSAgentOperator.on_can_bus_update)
        input_streams.filter(pylot_utils.is_waypoints_stream).add_callback(
            ERDOSAgentOperator.on_waypoints_update)
        input_streams.filter(pylot_utils.is_traffic_lights_stream).add_callback(
            ERDOSAgentOperator.on_traffic_lights_update)
        input_streams.filter(pylot_utils.is_obstacles_stream).add_callback(
            ERDOSAgentOperator.on_obstacles_update)
        input_streams.filter(pylot_utils.is_lidar_stream).add_callback(
            ERDOSAgentOperator.on_lidar_update)

        input_streams.add_completion_callback(
            ERDOSAgentOperator.on_notification)

        # Set no watermark on the output stream so that we do not
        # close the watermark loop with the carla operator.
        return [pylot_utils.create_control_stream()]

    def on_notification(self, msg):
        vehicle_transform = self._vehicle_transforms.popleft()
        point_cloud = self._point_clouds.popleft().tolist()

        tl_output = self._traffic_lights.popleft()
        traffic_lights = self.__transform_tl_output(tl_output, point_cloud)

        obstacles = self._obstacles.popleft()
        (pedestrians, vehicles) = self.__transform_detector_output(
            obstacles, point_cloud)

        self._logger.info("Timestamps {} {} {}".format(
            point_cloud.timestamp, tl_output.timestamp, obstacles.timestamp))

        speed_factor, state = self.__stop_for_agents(
            vehicle_transform,  self._wp_angle, self._wp_vector, vehicles,
            pedestrians, traffic_lights)

        control_msg = self.get_control_message(
            self._wp_angle, self._wp_angle_speed, speed_factor,
            self._vehicle_speed, Timestamp(coordinates=[0]))
        self.get_output_stream('control_stream').send(control_msg)

    def on_waypoints_update(self, msg):
        self._wp_angle = msg.wp_angle
        self._wp_vector = msg.wp_vector
        self._wp_angle_speed = msg.wp_angle_speed

    def on_can_bus_update(self, msg):
        self._vehicle_transforms.append(msg.data.transform)
        self._vehicle_speed = msg.data.forward_speed

    def on_traffic_lights_update(self, msg):
        self._logger.info("Traffic light update at {}".format(msg.timestamp))
        self._traffic_lights.append(msg)

    def on_obstacles_update(self, msg):
        self._logger.info("Obstacle update at {}".format(msg.timestamp))
        self._obstacles.append(msg)

    def on_lidar_update(self, msg):
        self._point_clouds.append(msg)

    def execute(self):
        self.spin()

    def __transform_to_3d(self, x, y, point_cloud):
        pos = get_3d_world_position(x,
                                    y,
                                    0.001,  # Setting so that it doesn't affect the calculation
                                    self._camera_transform,
                                    self._camera_width,
                                    self._camera_height,
                                    self._camera_fov)
        pos_3d = self.__find_closest_lidar_point(pos.y, pos.z, point_cloud)
        if pos_3d is None:
            self._logger.error(
                'Could not find lidar point for {} {}'.format(x, y))
        return pos_3d

    def __find_closest_lidar_point(self, y, z, point_cloud):
        closest_point = None
        dist = None
        for (px, py, pz) in point_cloud:
            py /= px
            pz /= px
            y_dist = abs(y - py)
            z_dist = abs(z - pz)
            # In front, and close to the point.
            if px > 0.0 and y_dist < 0.1 and z_dist < 0.1:
                if dist is None:
                    closest_point = (px, py, pz)
                    dist = y_dist + z_dist
                elif y_dist + z_dist < dist:
                    closest_point = (px, py, pz)
                    dist = y_dist + z_dist
        if closest_point:
            return simulation.utils.Location(closest_point[0],
                                             closest_point[1],
                                             closest_point[2])
        else:
            return None

    def __transform_tl_output(self, tls, point_cloud):
        traffic_lights = []
        for tl in tls.detected_objects:
            x = (tl.corners[0] + tl.corners[1]) / 2
            y = (tl.corners[2] + tl.corners[3]) / 2
            pos = self.__transform_to_3d(x, y, point_cloud)
            if pos:
                state = 0
                if tl.label is not 'Green':
                    state = 1
                traffic_lights.append((pos, state))
        return traffic_lights

    def __transform_detector_output(self, obstacles, point_cloud):
        vehicles = []
        pedestrians = []
        for detected_obj in obstacles.detected_objects:
            x = (detected_obj.corners[0] + detected_obj.corners[1]) / 2
            y = (detected_obj.corners[2] + detected_obj.corners[3]) / 2
            if detected_obj.label == 'person':
                pos = self.__transform_to_3d(x, y, point_cloud)
                if pos:
                    pedestrians.append(pos)
            elif (detected_obj.label == 'car' or
                  detected_obj.label == 'bicycle' or
                  detected_obj.label == 'motorcycle' or
                  detected_obj.label == 'bus' or
                  detected_obj.label == 'truck'):
                pos = self.__transform_to_3d(x, y, point_cloud)
                if pos:
                    vehicles.append(pos)
        return (pedestrians, vehicles)

    def __stop_for_agents(self,
                          vehicle_transform,
                          wp_angle,
                          wp_vector,
                          vehicles,
                          pedestrians,
                          traffic_lights):
        speed_factor = 1
        speed_factor_tl = 1
        speed_factor_p = 1
        speed_factor_v = 1

        for obs_vehicle_pos in vehicles:
            if agent_utils.is_vehicle_on_same_lane(
                    vehicle_transform, obs_vehicle_pos):
                new_speed_factor_v = agent_utils.stop_vehicle(
                    vehicle_transform, obs_vehicle_pos, wp_vector,
                    speed_factor_v, self._flags)
                speed_factor_v = min(speed_factor_v, new_speed_factor_v)

        for obs_ped_pos in pedestrians:
            if agent_utils.is_pedestrian_hitable(obs_ped_pos):
                new_speed_factor_p = agent_utils.stop_pedestrian(
                    vehicle_transform,
                    obs_ped_pos,
                    wp_vector,
                    speed_factor_p,
                    self._flags)
                speed_factor_p = min(speed_factor_p, new_speed_factor_p)

        for tl in traffic_lights:
            if (agent_utils.is_traffic_light_active(
                    vehicle_transform, tl[0]) and
                agent_utils.is_traffic_light_visible(
                    vehicle_transform, tl[0], self._flags)):
                tl_state = tl[1]
                new_speed_factor_tl = agent_utils.stop_traffic_light(
                    vehicle_transform,
                    tl[0],
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
