import math
import numpy as np
import time

from carla.sensor import Camera

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_csv_logging, setup_logging

import agent_utils
from planner.map import CarlaMap
from planner.waypointer import Waypointer
from pid_controller.pid import PID
from utils import get_3d_world_position


class ERDOSAgentOperator(Op):
    def __init__(self,
                 name,
                 city_name,
                 goal_location,
                 goal_orientation,
                 depth_camera_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(ERDOSAgentOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._map = CarlaMap(city_name)
        self._goal_location = goal_location
        self._goal_orientation = goal_orientation
        self._wp_num_steer = 0.9  # Select WP - Reverse Order: 1 - closest, 0 - furthest
        self._wp_num_speed = 0.4  # Select WP - Reverse Order: 1 - closest, 0 - furthest
        self._waypointer = Waypointer(city_name)
        self._pid = PID(p=self._flags.pid_p,
                        i=self._flags.pid_i,
                        d=self._flags.pid_d)
        self._world_transform = []
        self._depth_imgs = []
        self._traffic_lights = []
        self._obstacles = []
        self._vehicle_pos = None
        self._vehicle_acc = None
        self._vehicle_speed = None
        (self._depth_intrinsic, self._depth_transform, self._depth_img_size) = self.__setup_camera_tranforms(
            name=depth_camera_name, postprocessing='SemanticSegmentation')

    def __setup_camera_tranforms(self,
                                 name,
                                 postprocessing,
                                 field_of_view=90.0,
                                 image_size=(800, 600),
                                 position=(0.3, 0, 1.3),
                                 rotation_pitch=0,
                                 rotation_roll=0,
                                 rotation_yaw=0):
        camera = Camera(
            name,
            PostProcessing=postprocessing,
            FOV=field_of_view,
            ImageSizeX=image_size[0],
            ImageSizeY=image_size[1],
            PositionX=position[0],
            PositionY=position[1],
            PositionZ=position[2],
            RotationPitch=rotation_pitch,
            RotationRoll=rotation_roll,
            RotationYaw=rotation_yaw)

        image_width = image_size[0]
        image_height = image_size[1]
        # (Intrinsic) K Matrix
        intrinsic_mat = np.identity(3)
        intrinsic_mat[0][2] = image_width / 2
        intrinsic_mat[1][2] = image_height / 2
        intrinsic_mat[0][0] = intrinsic_mat[1][1] = image_width / (2.0 * math.tan(90.0 * math.pi / 360.0))
        return (intrinsic_mat, camera.get_unreal_transform(), (image_width, image_height))

    @staticmethod
    def setup_streams(input_streams, depth_camera_name):
        def is_traffic_lights_stream(stream):
            return stream.labels.get('traffic_lights', '') == 'true'

        def is_segmented_frame_stream(stream):
            return stream.labels.get('segmented', '') == 'true'

        def is_obstacles_stream(stream):
            return stream.labels.get('obstacles', '') == 'true'

        input_streams.filter_name(depth_camera_name).add_callback(
            ERDOSAgentOperator.on_depth_camera_update)

        # XXX(ionel): We get the exact position from the simulator.
        input_streams.filter_name('world_transform').add_callback(
            ERDOSAgentOperator.on_world_transform_update)
        input_streams.filter_name('vehicle_pos').add_callback(
            ERDOSAgentOperator.on_vehicle_pos_update)
        input_streams.filter_name('acceleration').add_callback(
            ERDOSAgentOperator.on_vehicle_acceleration_update)
        input_streams.filter_name('forward_speed').add_callback(
            ERDOSAgentOperator.on_forward_speed_update)

        input_streams.filter(is_traffic_lights_stream).add_callback(
            ERDOSAgentOperator.on_traffic_lights_update)
        input_streams.filter(is_segmented_frame_stream).add_callback(
            ERDOSAgentOperator.on_segmented_frame)
        input_streams.filter(is_obstacles_stream).add_callback(
            ERDOSAgentOperator.on_obstacles_update)

        return [DataStream(name='action_stream')]

    # TODO(ionel): Set the frequency programmatically.
    @frequency(10)
    def run_step(self):
        self._logger.info("Running step")
        # Return if we haven't yet received all vehicle info data.
        if (self._vehicle_pos is None or
            self._vehicle_acc is None or
            self._vehicle_speed is None):
            return

        if (len(self._obstacles) == 0 or
            len(self._traffic_lights) == 0 or
            len(self._depth_imgs) == 0 or
            len(self._world_transform) == 0):
            return

        self._logger.info("Timestamps {} {} {} {}".format(
            self._obstacles[0].timestamp,
            self._traffic_lights[0].timestamp,
            self._depth_imgs[0].timestamp,
            self._world_transform[0].timestamp))

        world_transform = self._world_transform[0].data
        self._world_transform = self._world_transform[1:]

        depth_img = self._depth_imgs[0].data
        self._depth_imgs = self._depth_imgs[1:]

        traffic_lights = []
        for (corners, score, label) in self._traffic_lights[0].data:
            x = (corners[0] + corners[1]) / 2
            y = (corners[2] + corners[3]) / 2
            (x3d, y3d, z3d) = get_3d_world_position(
                x, y, self._depth_img_size, depth_img, self._depth_transform, world_transform)
            state = 0
            if label is not 'Green':
                state = 1
            traffic_lights.append((x3d, y3d, state))
        self._traffic_lights = self._traffic_lights[1:]

        vehicles = []
        pedestrians = []
        for (corners, score, label) in self._obstacles[0].data:
            x = (corners[0] + corners[1]) / 2
            y = (corners[2] + corners[3]) / 2
            if label == 'person':
                (x3d, y3d, z3d) = get_3d_world_position(x, y,
                                                        self._depth_img_size,
                                                        depth_img,
                                                        self._depth_transform,
                                                        world_transform)
                pedestrians.append((x3d, y3d))
            elif label == 'car' or label == 'bicycle' or label == 'motorcycle' or label == 'bus' or label == 'truck':
                (x3d, y3d, z3d) = get_3d_world_position(x, y,
                                                        self._depth_img_size,
                                                        depth_img,
                                                        self._depth_transform,
                                                        world_transform)
                vehicles.append((x3d, y3d))
        self._obstacles = self._obstacles[1:]

        start_time = time.time()

        wp_angle, wp_vector, wp_angle_speed, wp_vector_speed = agent_utils.get_waypoints(
            self._goal_location, self._goal_orientation, self._vehicle_pos,
            self._waypointer, self._wp_num_steer, self._wp_num_speed)

        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},{}'.format(
            time_epoch_ms(), self.name, runtime))
        self._logger.info('Waypointer {} runtime {}'.format(
            self.name, runtime))

        speed_factor, state = self.__stop_for_agents(
            wp_angle, wp_vector, vehicles, pedestrians, traffic_lights)

        control = self.get_control(wp_angle, wp_angle_speed, speed_factor,
                                   self._vehicle_speed * 3.6)
        output_msg = Message(control, Timestamp(coordinates=[0]))
        self.get_output_stream('action_stream').send(output_msg)

    def on_world_transform_update(self, msg):
        self._world_transform.append(msg)

    def on_depth_camera_update(self, msg):
        self._depth_imgs.append(msg)

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

    def on_vehicle_pos_update(self, msg):
        self._logger.info("Received vehicle pos %s", msg)
        self._vehicle_pos = msg.data

    def on_vehicle_acceleration_update(self, msg):
        self._vehicle_acc = msg.data

    def on_forward_speed_update(self, msg):
        self._vehicle_speed = msg.data

    def execute(self):
        self.run_step()
        self.spin()

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

        return speed_factor, state

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
