from absl import flags
import math
import numpy as np

FLAGS = flags.FLAGS
flags.DEFINE_bool('stop_for_traffic_lights', True,
                  'True to enable traffic light stopping')
flags.DEFINE_bool('stop_for_pedestrians', True,
                  'True to enable pedestrian stopping')
flags.DEFINE_bool('stop_for_vehicles', True,
                  'True to enable vehicle stopping')
# Traffic light stopping parameters.
flags.DEFINE_integer('traffic_light_min_dist_thres', 9,
                     'Min distance threshold traffic light')
flags.DEFINE_integer('traffic_light_max_dist_thres', 20,
                     'Max distance threshold traffic light')
flags.DEFINE_float('traffic_light_angle_thres', 0.5,
                   'Traffic light angle threshold')
# Vehicle stopping parameters.
flags.DEFINE_integer('vehicle_distance_thres', 15,
                     'Vehicle distance threshold')
flags.DEFINE_float('vehicle_angle_thres', 0.4,
                   'Vehicle angle threshold')
# Pedestrian stopping parameters.
flags.DEFINE_integer('pedestrian_distance_hit_thres', 35,
                     'Pedestrian hit zone distance threshold')
flags.DEFINE_float('pedestrian_angle_hit_thres', 0.15,
                   'Pedestrian hit zone angle threshold')
flags.DEFINE_integer('pedestrian_distance_emergency_thres', 12,
                     'Pedestrian emergency zone distance threshold')
flags.DEFINE_float('pedestrian_angle_emergency_thres', 0.5,
                   'Pedestrian emergency zone angle threshold')
# PID controller parameters
flags.DEFINE_float('pid_p', 0.25, 'PID p parameter')
flags.DEFINE_float('pid_i', 0.20, 'PID i parameter')
flags.DEFINE_float('pid_d', 0.0, 'PID d parameter')
# Steering control parameters
flags.DEFINE_float('default_throttle', 0.0, 'Default throttle')
flags.DEFINE_float('throttle_max', 0.75, 'Max throttle')
flags.DEFINE_integer('target_speed', 36,
                     'Target speed, could be controlled by the speed limit')
flags.DEFINE_float('steer_gain', 0.7, 'Gain on computed steering angle')
flags.DEFINE_float('brake_strength', 1,
                   'Strength for applying brake; between 0 and 1')
flags.DEFINE_integer('coast_factor', 2, 'Factor to control coasting')


def get_waypoints(goal_location, goal_orientation, vehicle_pos,
                  waypointer, wp_num_steer, wp_num_speed):
    vehicle_x = vehicle_pos[0][0]
    vehicle_y = vehicle_pos[0][1]
    vehicle_ori_x = vehicle_pos[1][0]
    vehicle_ori_y = vehicle_pos[1][1]
    vehicle_ori_z = vehicle_pos[1][2]

    waypoints_world, waypoints, route = waypointer.get_next_waypoints(
        (vehicle_x, vehicle_y, 0.22),
        (vehicle_ori_x, vehicle_ori_y, vehicle_ori_z),
        goal_location,
        goal_orientation)

    if waypoints_world == []:
        waypoints_world = [[vehicle_x, vehicle_y, 0.22]]

    # Make a function, maybe util function to get the magnitues
    wp = [
        waypoints_world[int(wp_num_steer * len(waypoints_world))][0],
        waypoints_world[int(wp_num_steer * len(waypoints_world))][1]
    ]

    wp_vector, wp_mag = get_world_vec_dist(wp[0], wp[1], vehicle_x, vehicle_y)

    if wp_mag > 0:
        wp_angle = get_angle(wp_vector, [vehicle_ori_x, vehicle_ori_y])
    else:
        wp_angle = 0

    wp_speed = [
        waypoints_world[int(wp_num_speed * len(waypoints_world))][0],
        waypoints_world[int(wp_num_speed * len(waypoints_world))][1]
    ]

    wp_vector_speed, _ = get_world_vec_dist(
        wp_speed[0], wp_speed[1], vehicle_x, vehicle_y)

    wp_angle_speed = get_angle(
        wp_vector_speed, [vehicle_ori_x, vehicle_ori_y])

    return wp_angle, wp_vector, wp_angle_speed, wp_vector_speed


def get_world_vec_dist(x_dst, y_dst, x_src, y_src):
    vec = np.array([x_dst, y_dst] - np.array([x_src, y_src]))
    dist = math.sqrt(vec[0]**2 + vec[1]**2)
    return vec / dist, dist


def get_angle(vec_dst, vec_src):
    angle = (math.atan2(vec_dst[1], vec_dst[0]) -
             math.atan2(vec_src[1], vec_src[0]))
    if angle > math.pi:
        angle -= 2 * math.pi
    elif angle < -math.pi:
        angle += 2 * math.pi
    return angle


def is_pedestrian_hitable(pedestrian, city_map):
    x_pedestrian = pedestrian[0]
    y_pedestrian = pedestrian[1]
    return city_map.is_point_on_lane([x_pedestrian, y_pedestrian, 38])


def is_pedestrian_on_hit_zone(p_dist, p_angle):
    return (math.fabs(p_angle) < FLAGS.pedestrian_angle_hit_thres and
            p_dist < FLAGS.pedestrian_distance_hit_thres)


def is_pedestrian_on_near_hit_zone(p_dist, p_angle):
    return (math.fabs(p_angle) < FLAGS.pedestrian_angle_emergency_thres and
            p_dist < FLAGS.pedestrian_distance_emergency_thres)


def is_traffic_light_visible(vehicle_pos, tl_pos):
    _, tl_dist = get_world_vec_dist(
        vehicle_pos[0][0],
        vehicle_pos[0][1],
        tl_pos[0],
        tl_pos[1])
    return tl_dist > FLAGS.traffic_light_min_dist_thres

def is_traffic_light_active(vehicle_pos, tl_pos, city_map):
    x_vehicle = vehicle_pos[0][0]
    y_vehicle = vehicle_pos[0][1]
    tl_x = tl_pos[0]
    tl_y = tl_pos[1]

    def search_closest_lane_point(x_agent, y_agent, depth):
        step_size = 4
        if depth > 1:
            return None
        try:
            degrees = city_map.get_lane_orientation_degrees(
                [x_agent, y_agent, 38])
        except:
            return None
        if not city_map.is_point_on_lane([x_agent, y_agent, 38]):
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
        city_map.get_lane_orientation_degrees([x_vehicle, y_vehicle, 38])
        - city_map.get_lane_orientation_degrees(
            [closest_lane_point[0], closest_lane_point[1], 38])) < 1)


def stop_pedestrian(vehicle_pos, pedestrian, wp_vector, speed_factor_p):
    speed_factor_p_temp = 1
    x_pedestrian = pedestrian[0]
    y_pedestrian = pedestrian[1]
    p_vector, p_dist = get_world_vec_dist(
        x_pedestrian,
        y_pedestrian,
        vehicle_pos[0][0],
        vehicle_pos[0][1])
    p_angle = get_angle(p_vector, wp_vector)
    if is_pedestrian_on_hit_zone(p_dist, p_angle):
        speed_factor_p_temp = p_dist / (FLAGS.coast_factor * FLAGS.pedestrian_distance_hit_thres)
    if is_pedestrian_on_near_hit_zone(p_dist, p_angle):
        speed_factor_p_temp = 0
    if (speed_factor_p_temp < speed_factor_p):
        speed_factor_p = speed_factor_p_temp
    return speed_factor_p


def is_vehicle_on_same_lane(vehicle_pos, obs_vehicle, city_map):
    x_vehicle = vehicle_pos[0][0]
    y_vehicle = vehicle_pos[0][1]
    x_agent = obs_vehicle[0]
    y_agent = obs_vehicle[1]
    if city_map.is_point_on_intersection([x_agent, y_agent, 38]):
        return True
    return (math.fabs(
        city_map.get_lane_orientation_degrees([x_vehicle, y_vehicle, 38]) -
        city_map.get_lane_orientation_degrees([x_agent, y_agent, 38])) < 1)


def stop_vehicle(vehicle_pos, vehicle, wp_vector, speed_factor_v):
    x_vehicle = vehicle_pos[0][0]
    y_vehicle = vehicle_pos[0][1]
    x_agent = vehicle[0]
    y_agent = vehicle[1]
    speed_factor_v_temp = 1
    v_vector, v_dist = get_world_vec_dist(x_agent, y_agent, x_vehicle, y_vehicle)
    v_angle = get_angle(v_vector, wp_vector)

    if ((-0.5 * FLAGS.vehicle_angle_thres / FLAGS.coast_factor <
         v_angle < FLAGS.vehicle_angle_thres / FLAGS.coast_factor and
         v_dist < FLAGS.vehicle_distance_thres * FLAGS.coast_factor) or
        (-0.5 * FLAGS.vehicle_angle_thres / FLAGS.coast_factor <
         v_angle < FLAGS.vehicle_angle_thres and
         v_dist < FLAGS.vehicle_distance_thres)):
        speed_factor_v_temp = v_dist / (
            FLAGS.coast_factor * FLAGS.vehicle_distance_thres)

    if (-0.5 * FLAGS.vehicle_angle_thres * FLAGS.coast_factor <
        v_angle < FLAGS.vehicle_angle_thres * FLAGS.coast_factor and
        v_dist < FLAGS.vehicle_distance_thres / FLAGS.coast_factor):
        speed_factor_v_temp = 0

    if speed_factor_v_temp < speed_factor_v:
        speed_factor_v = speed_factor_v_temp

    return speed_factor_v


def stop_traffic_light(vehicle_pos, tl_pos, tl_state, wp_vector, wp_angle, speed_factor_tl):
    x_vehicle = vehicle_pos[0][0]
    y_vehicle = vehicle_pos[0][1]
    speed_factor_tl_temp = 1
    if tl_state != 0:  # Not green
        tl_x = tl_pos[0]
        tl_y = tl_pos[1]
        tl_vector, tl_dist = get_world_vec_dist(tl_x, tl_y, x_vehicle, y_vehicle)
        tl_angle = get_angle(tl_vector, wp_vector)

        if ((0 < tl_angle < FLAGS.traffic_light_angle_thres / FLAGS.coast_factor and
             tl_dist < FLAGS.traffic_light_max_dist_thres * FLAGS.coast_factor) or
            (0 < tl_angle < FLAGS.traffic_light_angle_thres and
             tl_dist < FLAGS.traffic_light_max_dist_thres) and
            math.fabs(wp_angle) < 0.2):

            speed_factor_tl_temp = tl_dist / (
                FLAGS.coast_factor *
                FLAGS.traffic_light_max_dist_thres)

        if ((0 < tl_angle < FLAGS.traffic_light_angle_thres * FLAGS.coast_factor and
             tl_dist < FLAGS.traffic_light_max_dist_thres / FLAGS.coast_factor) and
            math.fabs(wp_angle) < 0.2):
            speed_factor_tl_temp = 0

        if (speed_factor_tl_temp < speed_factor_tl):
            speed_factor_tl = speed_factor_tl_temp

    return speed_factor_tl
