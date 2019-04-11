from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_bool('replay', False,
                  ('True if run in replay mode, otherwise run '
                   'Carla in server mode using `./CarlaUE4.sh -carla-server`'))
flags.DEFINE_string('log_file_name', None, 'Name of the log file')
flags.DEFINE_bool('ground_agent_operator', True,
                  'True to use the ground truth controller')

# Modules to enable.
flags.DEFINE_bool('segmentation', False,
                  'True to enable segmantation operator')
flags.DEFINE_bool('segmentation_gpu', True,
                  'True, if segmentation should use a GPU')
flags.DEFINE_string('segmentation_type', 'drn', 'Segmentation type: drn | dla')
flags.DEFINE_bool('obj_detection', False,
                  'True to enable object detection operator')
flags.DEFINE_string(
    'detector_model_path',
    'dependencies/data/ssd_mobilenet_v1_coco_2018_01_28/frozen_inference_graph.pb',
    'Path to the model protobuf')
#DETECTOR_MODEL_PATH = 'dependencies/faster_rcnn_resnet101_coco_2018_01_28/frozen_inference_graph.pb'
flags.DEFINE_float('detector_min_score_threshold', 0.5,
                   'Min score threshold for bounding box')
flags.DEFINE_string('path_coco_labels', 'dependencies/data/coco.names',
                    'Path to the COCO labels')
flags.DEFINE_bool('obj_tracking', False,
                  'True to enable object tracking operator')
flags.DEFINE_string('tracker_type', 'cv2', 'Tracker type: cv2 | crt')
flags.DEFINE_bool('fusion', False, 'True to enable fusion operator')
flags.DEFINE_bool('traffic_light_det', False,
                  'True to enable traffic light detection operator')
flags.DEFINE_string(
    'traffic_light_det_model_path',
    'dependencies/data/traffic_light_det_inference_graph.pb',
    'Path to the traffic light model protobuf')
flags.DEFINE_float('traffic_light_det_min_score_threshold', 0.3,
                   'Min score threshold for bounding box')

# Agent flags.
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

# Carla flags.
flags.DEFINE_string('carla_host', 'localhost', 'Carla host.')
flags.DEFINE_integer('carla_port', 2000, 'Carla port.')
flags.DEFINE_bool('carla_synchronous_mode', True,
                  'Run Carla in synchronous mode.')
flags.DEFINE_integer('carla_step_frequency', 10,
                     'Frequency of Carla data readings.')
flags.DEFINE_integer('carla_num_vehicles', 20, 'Carla num vehicles.')
flags.DEFINE_integer('carla_num_pedestrians', 40, 'Carla num pedestrians.')
flags.DEFINE_bool('carla_high_quality', False,
                  'True to enable high quality Carla simulations.')
flags.DEFINE_integer('carla_weather', 2,
                     'Carla weather preset; between 1 and 14')
flags.DEFINE_bool('carla_random_player_start', True,
                  'True to randomly assign a car to the player')
flags.DEFINE_integer('carla_start_player_num', 0,
                     'Number of the assigned start player')

# Visualizing operators
flags.DEFINE_bool('visualize_depth_camera', False,
                  'True to enable depth camera video operator')
flags.DEFINE_bool('visualize_lidar', False,
                  'True to enable CARLA Lidar visualizer operator')
flags.DEFINE_bool('visualize_rgb_camera', True,
                  'True to enable RGB camera video operator')
flags.DEFINE_bool('visualize_segmentation', False,
                  'True to enable CARLA segmented video operator')
flags.DEFINE_bool('visualize_ground_obstacles', False,
                  'True to enable visualization of ground obstacles')
flags.DEFINE_bool('visualize_tracker_output', False,
                  'True to enable visualization of tracker output')
flags.DEFINE_bool('visualize_segmentation_output', False,
                  'True to enable visualization of segmentation output')
flags.DEFINE_bool('visualize_detector_output', False,
                  'True to enable visualization of detector output')
flags.DEFINE_bool('visualize_traffic_light_output', False,
                  'True to enable visualization of traffic light output')

# Recording operators
flags.DEFINE_bool('record_depth_camera', False, 'True to record depth camera')
flags.DEFINE_bool('record_lidar', False, 'True to record lidar')
flags.DEFINE_bool('record_rgb_camera', False, 'True to record RGB camera')
flags.DEFINE_bool(
    'record_ground_truth', False,
    'True to carla data (e.g., vehicle position, traffic lights)')

# Other flags
flags.DEFINE_integer('num_cameras', 5, 'Number of cameras.')
flags.DEFINE_bool('evaluate_obj_detection', False,
                  'True to enable object detection accuracy evaluation')
flags.DEFINE_bool('evaluate_segmentation', False,
                  'True to enable segmentation evaluation')

# Flag validators.
flags.register_validator('framework',
                         lambda value: value == 'ros' or value == 'ray',
                         message='--framework must be: ros | ray')
flags.register_multi_flags_validator(
    ['replay', 'evaluate_obj_detection'],
    lambda flags_dict: not (flags_dict['replay'] and flags_dict['evaluate_obj_detection']),
    message='--evaluate_obj_detection cannot be set when --replay is set')
flags.register_multi_flags_validator(
    ['replay', 'fusion'],
    lambda flags_dict: not (flags_dict['replay'] and flags_dict['fusion']),
    message='--fusion cannot be set when --replay is set')
flags.register_multi_flags_validator(
    ['ground_agent_operator', 'obj_detection', 'traffic_light_det', 'segmentation'],
    lambda flags_dict: (flags_dict['ground_agent_operator'] or
                        (flags_dict['obj_detection'] and
                         flags_dict['traffic_light_det'] and
                         flags_dict['segmentation'])),
    message='ERDOS agent requires obj detection, segmentation and traffic light detection')


def tracker_flag_validator(flags_dict):
    if flags_dict['obj_tracking']:
        return flags_dict['obj_detection']
    return True

flags.register_multi_flags_validator(
    ['obj_detection', 'obj_tracking'],
    tracker_flag_validator,
    message='--obj_detection must be set if --obj_tracking is set')


def detector_accuracy_validator(flags_dict):
    if flags_dict['evaluate_obj_detection']:
        return flags_dict['obj_detection']
    return True

flags.register_multi_flags_validator(
    ['obj_detection', 'evaluate_obj_detection'],
    detector_accuracy_validator,
    message='--obj_detection mustg be set if --evaluate_obj_detection is set')
