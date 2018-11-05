import math
import numpy as np

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging

from planner.map import CarlaMap
from planner.waypointer import Waypointer
from pid_controller.pid import PID


class AgentOperator(Op):
    def __init__(self, name, city_name, goal_location, goal_orientation):
        super(AgentOperator, self).__init__(name)
        self._logger = setup_logging(self.name)
        self._map = CarlaMap(city_name)
        self._goal_location = goal_location
        self._goal_orientation = goal_orientation
        self.param = {
            'stop4TL': True,  # Stop for traffic lights
            'stop4P': True,  # Stop for pedestrians
            'stop4V': True,  # Stop for vehicles
            'coast_factor': 2,  # Factor to control coasting
            'tl_min_dist_thres': 9,  # Distance Threshold Traffic Light
            'tl_max_dist_thres': 20,  # Distance Threshold Traffic Light
            'tl_angle_thres': 0.5,  # Angle Threshold Traffic Light
            'p_dist_hit_thres': 35,  # Distance Threshold Pedestrian
            'p_angle_hit_thres': 0.15,  # Angle Threshold Pedestrian
            'p_dist_eme_thres': 12,  # Distance Threshold Pedestrian
            'p_angle_eme_thres': 0.5,  # Angle Threshold Pedestrian
            'v_dist_thres': 15,  # Distance Threshold Vehicle
            'v_angle_thres': 0.40,  # Angle Threshold Vehicle
            'default_throttle': 0.0,  # Default Throttle
            'default_brake': 0.0,  # Default Brake
            'steer_gain': 0.7,  # Gain on computed steering angle
            'brake_strength': 1,  # Strength for applying brake - Value between 0 and 1
            'pid_p': 0.25,  # PID speed controller parameters
            'pid_i': 0.20,
            'pid_d': 0.00,
            'target_speed': 36,  # Target speed - could be controlled by speed limit
            'throttle_max': 0.75,
        }
        self.wp_num_steer = 0.9  # Select WP - Reverse Order: 1 - closest, 0 - furthest
        self.wp_num_speed = 0.4  # Select WP - Reverse Order: 1 - closest, 0 - furthest
        self.waypointer = Waypointer(city_name)
        self.pid = PID(
            p=self.param['pid_p'],
            i=self.param['pid_i'],
            d=self.param['pid_d'])
        self.vehicle_pos = None
        self.vehicle_acc = None
        self.vehicle_speed = None
        self.pedestrians = []
        self.vehicles = []
        self.traffic_lights = []
        self.traffic_signs = []

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter_name('vehicle_pos').add_callback(
            AgentOperator.on_vehicle_pos_update)
        input_streams.filter_name('acceleration').add_callback(
            AgentOperator.on_vehicle_acceleration_update)
        input_streams.filter_name('forward_speed').add_callback(
            AgentOperator.on_forward_speed_update)
        input_streams.filter_name('pedestrians').add_callback(
            AgentOperator.on_pedestrians_update)
        input_streams.filter_name('vehicles').add_callback(
            AgentOperator.on_vehicles_update)
        input_streams.filter_name('traffic_lights').add_callback(
            AgentOperator.on_traffic_lights_update)
        input_streams.filter_name('traffic_signs').add_callback(
            AgentOperator.on_traffic_signs_update)
        return [DataStream(name='action_stream')]

    @frequency(5)
    def run_step(self):
        self._logger.info("Running step")
        if (self.vehicle_pos is None or self.vehicle_acc is None
                or self.vehicle_speed is None or self.pedestrians == []
                or self.vehicles == [] or self.traffic_lights == []):
            return
        vehicle_x = self.vehicle_pos[0][0]
        vehicle_y = self.vehicle_pos[0][1]
        vehicle_ori_x = self.vehicle_pos[1][0]
        vehicle_ori_y = self.vehicle_pos[1][1]
        vehicle_ori_z = self.vehicle_pos[1][2]

        waypoints_world, waypoints, route = self.waypointer.get_next_waypoints(
            (vehicle_x, vehicle_y, 0.22),
            (vehicle_ori_x, vehicle_ori_y, vehicle_ori_z), self._goal_location,
            self._goal_orientation)

        if waypoints_world == []:
            waypoints_world = [[vehicle_x, vehicle_y, 0.22]]

        # Make a function, maybe util function to get the magnitues
        wp = [
            waypoints_world[int(self.wp_num_steer * len(waypoints_world))][0],
            waypoints_world[int(self.wp_num_steer * len(waypoints_world))][1]
        ]

        wp_vector, wp_mag = self.get_vec_dist(wp[0], wp[1], vehicle_x,
                                              vehicle_y)

        if wp_mag > 0:
            wp_angle = self.get_angle(wp_vector,
                                      [vehicle_ori_x, vehicle_ori_y])
        else:
            wp_angle = 0

        wp_speed = [
            waypoints_world[int(self.wp_num_speed * len(waypoints_world))][0],
            waypoints_world[int(self.wp_num_speed * len(waypoints_world))][1]
        ]

        wp_vector_speed, _ = self.get_vec_dist(wp_speed[0], wp_speed[1],
                                               vehicle_x, vehicle_y)

        wp_angle_speed = self.get_angle(wp_vector_speed,
                                        [vehicle_ori_x, vehicle_ori_y])

        speed_factor, state = self.stop_for_agents(
            wp_angle, wp_vector, self.vehicles, self.pedestrians,
            self.traffic_lights)
        control = self.get_control(wp_angle, wp_angle_speed, speed_factor,
                                   self.vehicle_speed * 3.6)
        output_msg = Message(control, Timestamp(coordinates=[0]))
        self.get_output_stream('action_stream').send(output_msg)

    def on_vehicle_pos_update(self, msg):
        self._logger.info("Received vehicle pos %s", msg)
        self.vehicle_pos = msg.data

    def on_vehicle_acceleration_update(self, msg):
        self.vehicle_acc = msg.data

    def on_forward_speed_update(self, msg):
        self.vehicle_speed = msg.data

    def on_pedestrians_update(self, msg):
        self.pedestrians = msg.data

    def on_vehicles_update(self, msg):
        self.vehicles = msg.data

    def on_traffic_lights_update(self, msg):
        self.traffic_lights = msg.data

    def on_traffic_signs_update(self, msg):
        self.traffic_signs = msg.data

    def is_traffic_light_visible(self, traffic_light):
        _, tl_dist = self.get_vec_dist(self.vehicle_pos[0][0],
                                       self.vehicle_pos[0][1],
                                       traffic_light.position.location[0],
                                       traffic_light.position.location[1])
        return tl_dist > self.param['tl_min_dist_thres']

    def is_traffic_light_active(self, traffic_light):
        x_vehicle = self.vehicle_pos[0][0]
        y_vehicle = self.vehicle_pos[0][1]
        tl_x = traffic_light.position.location[0]
        tl_y = traffic_light.position.location[1]

        def search_closest_lane_point(x_agent, y_agent, depth):
            step_size = 4
            if depth > 1:
                return None
            try:
                degrees = self._map.get_lane_orientation_degrees(
                    [x_agent, y_agent, 38])
            except:
                return None
            if not self._map.is_point_on_lane([x_agent, y_agent, 38]):
                result = search_closest_lane_point(x_agent + step_size,
                                                   y_agent, depth + 1)
                if result is not None:
                    return result
                result = search_closest_lane_point(
                    x_agent, y_agent + step_size, depth + 1)
                if result is not None:
                    return result
                result = search_closest_lane_point(
                    x_agent + step_size, y_agent + step_size, depth + 1)
                if result is not None:
                    return result
                result = search_closest_lane_point(
                    x_agent + step_size, y_agent - step_size, depth + 1)
                if result is not None:
                    return result
                result = search_closest_lane_point(
                    x_agent - step_size, y_agent + step_size, depth + 1)
                if result is not None:
                    return result
                result = search_closest_lane_point(x_agent - step_size,
                                                   y_agent, depth + 1)
                if result is not None:
                    return result
                result = search_closest_lane_point(
                    x_agent, y_agent - step_size, depth + 1)
                if result is not None:
                    return result
                result = search_closest_lane_point(
                    x_agent - step_size, y_agent - step_size, depth + 1)
                if result is not None:
                    return result
            else:
                if degrees < 6:
                    return [x_agent, y_agent]
                else:
                    return None

        closest_lane_point = search_closest_lane_point(tl_x, tl_y, 0)
        return (math.fabs(
            self._map.get_lane_orientation_degrees([x_vehicle, y_vehicle, 38])
            - self._map.get_lane_orientation_degrees(
                [closest_lane_point[0], closest_lane_point[1], 38])) < 1)

    def stop_traffic_light(self, tl, wp_vector, wp_angle, speed_factor_tl):
        x_vehicle = self.vehicle_pos[0][0]
        y_vehicle = self.vehicle_pos[0][1]
        speed_factor_tl_temp = 1
        if tl.state != 0:  # Not green
            tl_x = tl.position.location[0]
            tl_y = tl.position.location[1]
            tl_vector, tl_dist = self.get_vec_dist(tl_x, tl_y, x_vehicle,
                                                   y_vehicle)
            tl_angle = self.get_angle(tl_vector, wp_vector)

            if ((0 < tl_angle <
                 self.param['tl_angle_thres'] / self.param['coast_factor']
                 and tl_dist <
                 self.param['tl_max_dist_thres'] * self.param['coast_factor'])
                    or (0 < tl_angle < self.param['tl_angle_thres']
                        and tl_dist < self.param['tl_max_dist_thres'])
                    and math.fabs(wp_angle) < 0.2):

                speed_factor_tl_temp = tl_dist / (
                    self.param['coast_factor'] *
                    self.param['tl_max_dist_thres'])

            if ((0 < tl_angle <
                 self.param['tl_angle_thres'] * self.param['coast_factor']
                 and tl_dist <
                 self.param['tl_max_dist_thres'] / self.param['coast_factor'])
                    and math.fabs(wp_angle) < 0.2):
                speed_factor_tl_temp = 0

            if (speed_factor_tl_temp < speed_factor_tl):
                speed_factor_tl = speed_factor_tl_temp

        return speed_factor_tl

    def is_pedestrian_hitable(self, pedestrian):
        x_pedestrian = pedestrian.position.location[0]
        y_pedestrian = pedestrian.position.location[1]
        return self._map.is_point_on_lane([x_pedestrian, y_pedestrian, 38])

    def is_vehicle_on_same_lane(self, vehicle):
        x_vehicle = self.vehicle_pos[0][0]
        y_vehicle = self.vehicle_pos[0][1]
        x_agent = vehicle.position.location[0]
        y_agent = vehicle.position.location[1]
        if self._map.is_point_on_intersection([x_agent, y_agent, 38]):
            return True
        return (math.fabs(
            self._map.get_lane_orientation_degrees([x_vehicle, y_vehicle, 38])
            - self._map.get_lane_orientation_degrees([x_agent, y_agent, 38])) <
                1)

    def is_pedestrian_on_hit_zone(self, p_dist, p_angle):
        return (math.fabs(p_angle) < self.param['p_angle_hit_thres']
                and p_dist < self.param['p_dist_hit_thres'])

    def is_pedestrian_on_near_hit_zone(self, p_dist, p_angle):
        return (math.fabs(p_angle) < self.param['p_angle_eme_thres']
                and p_dist < self.param['p_dist_eme_thres'])

    def stop_pedestrian(self, pedestrian, wp_vector, speed_factor_p):
        speed_factor_p_temp = 1
        x_pedestrian = pedestrian.position.location[0]
        y_pedestrian = pedestrian.position.location[1]
        p_vector, p_dist = self.get_vec_dist(x_pedestrian, y_pedestrian,
                                             self.vehicle_pos[0][0],
                                             self.vehicle_pos[0][1])
        p_angle = self.get_angle(p_vector, wp_vector)
        if self.is_pedestrian_on_hit_zone(p_dist, p_angle):
            speed_factor_p_temp = p_dist / (
                self.param['coast_factor'] * self.param['p_dist_hit_thres'])
        if self.is_pedestrian_on_near_hit_zone(p_dist, p_angle):
            speed_factor_p_temp = 0
        if (speed_factor_p_temp < speed_factor_p):
            speed_factor_p = speed_factor_p_temp
        return speed_factor_p

    def stop_vehicle(self, vehicle, wp_vector, speed_factor_v):
        x_vehicle = self.vehicle_pos[0][0]
        y_vehicle = self.vehicle_pos[0][1]
        x_agent = vehicle.position.location[0]
        y_agent = vehicle.position.location[1]
        speed_factor_v_temp = 1
        v_vector, v_dist = self.get_vec_dist(x_agent, y_agent, x_vehicle,
                                             y_vehicle)
        v_angle = self.get_angle(v_vector, wp_vector)

        if ((-0.5 * self.param['v_angle_thres'] / self.param['coast_factor'] <
             v_angle < self.param['v_angle_thres'] / self.param['coast_factor']
             and
             v_dist < self.param['v_dist_thres'] * self.param['coast_factor'])
                or
            (-0.5 * self.param['v_angle_thres'] / self.param['coast_factor'] <
             v_angle < self.param['v_angle_thres']
             and v_dist < self.param['v_dist_thres'])):
            speed_factor_v_temp = v_dist / (
                self.param['coast_factor'] * self.param['v_dist_thres'])

        if (-0.5 * self.param['v_angle_thres'] * self.param['coast_factor'] <
                v_angle <
                self.param['v_angle_thres'] * self.param['coast_factor']
                and v_dist <
                self.param['v_dist_thres'] / self.param['coast_factor']):
            speed_factor_v_temp = 0

        if speed_factor_v_temp < speed_factor_v:
            speed_factor_v = speed_factor_v_temp

        return speed_factor_v

    def stop_for_agents(self, wp_angle, wp_vector, vehicles, pedestrians,
                        traffic_lights):
        speed_factor = 1
        speed_factor_tl = 1
        speed_factor_p = 1
        speed_factor_v = 1

        for vehicle in vehicles:
            if self.param['stop4V'] and self.is_vehicle_on_same_lane(vehicle):
                new_speed_factor_v = self.stop_vehicle(vehicle, wp_vector,
                                                       speed_factor_v)
                speed_factor_v = min(speed_factor_v, new_speed_factor_v)

        for pedestrian in pedestrians:
            if self.param['stop4P'] and self.is_pedestrian_hitable(pedestrian):
                new_speed_factor_p = self.stop_pedestrian(
                    pedestrian, wp_vector, speed_factor_p)
                speed_factor_p = min(speed_factor_p, new_speed_factor_p)

        for tl in traffic_lights:
            if self.param['stop4TL'] and self.is_traffic_light_active(
                    tl) and self.is_traffic_light_visible(tl):
                new_speed_factor_tl = self.stop_traffic_light(
                    tl, wp_vector, wp_angle, speed_factor_tl)
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

    def get_vec_dist(self, x_dst, y_dst, x_src, y_src):
        vec = np.array([x_dst, y_dst] - np.array([x_src, y_src]))
        dist = math.sqrt(vec[0]**2 + vec[1]**2)
        return vec / dist, dist

    def get_angle(self, vec_dst, vec_src):
        angle = math.atan2(vec_dst[1], vec_dst[0]) - math.atan2(
            vec_src[1], vec_src[0])
        if angle > math.pi:
            angle -= 2 * math.pi
        elif angle < -math.pi:
            angle += 2 * math.pi
        return angle

    def get_control(self, wp_angle, wp_angle_speed, speed_factor,
                    current_speed):
        current_speed = max(current_speed, 0)
        steer = self.param['steer_gain'] * wp_angle
        if steer > 0:
            steer = min(steer, 1)
        else:
            steer = max(steer, -1)

        # Don't go to fast around corners
        if math.fabs(wp_angle_speed) < 0.1:
            target_speed_adjusted = self.param['target_speed'] * speed_factor
        elif math.fabs(wp_angle_speed) < 0.5:
            target_speed_adjusted = 20 * speed_factor
        else:
            target_speed_adjusted = 15 * speed_factor

        self.pid.target = target_speed_adjusted
        pid_gain = self.pid(feedback=current_speed)
        throttle = min(
            max(self.param['default_throttle'] - 1.3 * pid_gain, 0),
            self.param['throttle_max'])

        if pid_gain > 0.5:
            brake = min(0.35 * pid_gain * self.param['brake_strength'], 1)
        else:
            brake = 0

        return {
            'steer': steer,
            'throttle': throttle,
            'brake': brake,
            'hand_brake': False,
            'reverse': False
        }
