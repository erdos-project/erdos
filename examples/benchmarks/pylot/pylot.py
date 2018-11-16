import os
import sys
from absl import app
from absl import flags

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from camera_operator import CameraOperator
from lidar_operator import LidarOperator
from detection_operator import DetectionOperator
from fusion_operator import FusionOperator
from segmentation_operator import SegmentationOperator
from depth_camera_operator import DepthCameraOperator
from imu_operator import IMUOperator
from gps_operator import GPSOperator
from traffic_sign_det_operator import TrafficSignDetOperator
from lane_det_operator import LaneDetOperator
from radar_operator import RadarOperator
from intersection_det_operator import IntersectionDetOperator
from traffic_light_det_operator import TrafficLightDetOperator
from motion_planner_operator import MotionPlannerOperator
from mission_planner_operator import MissionPlannerOperator
from slam_operator import SLAMOperator
from mapping_operator import MappingOperator
from tracker_operator import TrackerOperator
from prediction_operator import PredictionOperator

import erdos.graph

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_bool('side_cameras', False, 'True to enable side cameras')
flags.DEFINE_bool('rear_cameras', False, 'True to enable rear cameras')
flags.DEFINE_string('front_camera_locations',
                    'front_left,front_center,front_right',
                    'Comma-separated list of locations')


def add_camera_op(graph, camera_name):
    return graph.add(
        CameraOperator, name=camera_name, setup_args={'op_name': camera_name})


def add_depth_camera_op(graph, depth_camera_name):
    return graph.add(
        DepthCameraOperator,
        name=depth_camera_name,
        setup_args={'op_name': depth_camera_name})


def add_radar_op(graph, radar_name):
    return graph.add(
        RadarOperator, name=radar_name, setup_args={'op_name': radar_name})


def add_detector_op(graph, detector_name):
    return graph.add(
        DetectionOperator,
        name=detector_name,
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': detector_name})


def add_intersection_det_op(graph, name):
    return graph.add(
        IntersectionDetOperator,
        name=name,
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': name})


def add_tracker_op(graph, name):
    return graph.add(
        TrackerOperator,
        name=name,
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': name})


def add_traffic_light_det_op(graph, name):
    return graph.add(
        TrafficLightDetOperator,
        name=name,
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': name})


def add_traffic_sign_det_op(graph, name):
    return graph.add(
        TrafficSignDetOperator,
        name=name,
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': name})


def add_segmentation_op(graph, name):
    return graph.add(
        SegmentationOperator,
        name=name,
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': name})


def add_lane_det_op(graph, name):
    return graph.add(
        LaneDetOperator,
        name=name,
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': name})


def add_front_camera_processing_graph(graph, location):
    camera_op = add_camera_op(graph, 'camera_' + location)
    obj_det_op = add_detector_op(graph, 'obj_det_' + location)
    segmentation_op = add_segmentation_op(graph, 'segmentation_' + location)
    traffic_light_det_op = add_traffic_light_det_op(
        graph, 'traffic_light_det_' + location)
    traffic_sign_det_op = add_traffic_sign_det_op(
        graph, 'traffic_sign_det_' + location)
    intersection_det_op = add_intersection_det_op(
        graph, 'intersection_det_' + location)
    lane_det_op = add_lane_det_op(graph, 'lane_det_' + location)
    obj_tracker_op = add_tracker_op(graph, 'obj_tracker_' + location)
    # TODO(ionel): Vary number of trackers depending on the number of detected
    # objects.
    graph.connect([camera_op], [
        obj_det_op, segmentation_op, traffic_light_det_op, traffic_sign_det_op,
        intersection_det_op
    ])
    graph.connect([segmentation_op], [lane_det_op])
    graph.connect([camera_op, obj_det_op], [obj_tracker_op])
    return [
        obj_tracker_op, lane_det_op, traffic_light_det_op, intersection_det_op,
        traffic_sign_det_op
    ]


def add_side_camera_processing_graph(graph, location):
    camera_op = add_camera_op(graph, 'camera_' + location)
    obj_det_op = add_detector_op(graph, 'obj_det_' + location)
    segmentation_op = add_segmentation_op(graph, 'segmentation_' + location)
    traffic_light_det_op = add_traffic_light_det_op(
        graph, 'traffic_light_det_' + location)
    intersection_det_op = add_intersection_det_op(
        graph, 'intersection_det_' + location)
    lane_det_op = add_lane_det_op(graph, 'lane_det_' + location)
    obj_tracker_op = add_tracker_op(graph, 'obj_tracker_' + location)
    graph.connect([camera_op], [
        obj_det_op, segmentation_op, traffic_light_det_op, intersection_det_op
    ])
    graph.connect([segmentation_op], [lane_det_op])
    graph.connect([camera_op, obj_det_op], [obj_tracker_op])
    return [
        obj_tracker_op, lane_det_op, traffic_light_det_op, intersection_det_op
    ]


def add_rear_camera_processing_graph(graph, location):
    camera_op = add_camera_op(graph, 'camera_' + location)
    obj_det_op = add_detector_op(graph, 'obj_det_' + location)
    obj_tracker_op = add_tracker_op(graph, 'obj_tracker_' + location)
    graph.connect([camera_op], [obj_det_op])
    graph.connect([camera_op, obj_det_op], [obj_tracker_op])
    return [obj_tracker_op]


def add_localization_graph(graph, front_locations, tracker_ops):
    # 1 LIDAR, 100000 points per point cloud.
    lidar_op = graph.add(
        LidarOperator,
        name='lidar',
        init_args={'num_points': 100000},
        setup_args={'op_name': 'lidar'})

    # 1 GPS
    gps_op = graph.add(GPSOperator, name='GPS')

    # 1 IMU
    imu_op = graph.add(IMUOperator, name='IMU')

    # 4 short range radars
    short_radars = ['front_left', 'front_right', 'rear_left', 'rear_right']
    short_radar_ops = []
    for location in short_radars:
        radar_name = 'short_radar_' + location
        short_radar_ops.append(add_radar_op(graph, radar_name))

    # 1 long range radar
    long_radar_op = add_radar_op(graph, 'long_radar')

    # 1 SLAM operator.
    slam_op = graph.add(
        SLAMOperator,
        name='SLAM',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        })

    # 3 depth cameras
    depth_camera_ops = []
    for location in front_locations:
        depth_camera_ops.append(
            add_depth_camera_op(graph, 'depth_camera_' + location))

    # fusion operator
    fusion_op = graph.add(
        FusionOperator,
        name='fusion',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        })

    graph.connect([lidar_op, long_radar_op, gps_op, imu_op], [slam_op])
    graph.connect(
        [slam_op, lidar_op] + tracker_ops + short_radar_ops + depth_camera_ops,
        [fusion_op])
    return (slam_op, fusion_op)


def add_prediction_graph(graph, tracker_ops):
    prediction_op = graph.add(
        PredictionOperator,
        name='prediction',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        })
    graph.connect([prediction_op], tracker_ops)
    return [prediction_op]


def main(argv):
    graph = erdos.graph.get_current_graph()

    front_locations = FLAGS.front_camera_locations.split(',')

    side_locations = []
    if FLAGS.side_cameras:
        side_locations = ['side_left', 'side_right']

    rear_locations = []
    if FLAGS.rear_cameras:
        rear_locations = ['rear_left', 'rear_center', 'rear_right']

    tracker_ops = []
    lane_det_ops = []
    traffic_light_det_ops = []
    intersection_det_ops = []
    traffic_sign_det_ops = []
    for location in front_locations:
        ops = add_front_camera_processing_graph(graph, location)
        tracker_ops.append(ops[0])
        lane_det_ops.append(ops[1])
        traffic_light_det_ops.append(ops[2])
        intersection_det_ops.append(ops[3])
        traffic_sign_det_ops.append(ops[4])

    for location in side_locations:
        ops = add_side_camera_processing_graph(graph, location)
        tracker_ops.append(ops[0])
        lane_det_ops.append(ops[1])
        traffic_light_det_ops.append(ops[2])
        intersection_det_ops.append(ops[3])
        traffic_sign_det_ops.append(ops[4])

    for location in rear_locations:
        ops = add_rear_camera_processing_graph(graph, location)
        tracker_ops.append(ops[0])

    # TODO(ionel): Plugin mapping operator.
    # mapping_op = graph.add(MappingOperator, name='mapping')

    # 1 mission planner operator.
    mission_planner_op = graph.add(
        MissionPlannerOperator,
        name='mission_planner',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        })
    # 1 motion planner operator.
    motion_planner_op = graph.add(
        MotionPlannerOperator,
        name='motion_planner',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        })

    (slam_op, fusion_op) = add_localization_graph(graph, front_locations,
                                                  tracker_ops)
    prediction_op = add_prediction_graph(graph, tracker_ops)[0]

    graph.connect([slam_op], [mission_planner_op])
    graph.connect(
        [mission_planner_op, fusion_op, prediction_op] + lane_det_ops +
        traffic_light_det_ops + intersection_det_ops + traffic_sign_det_ops,
        [motion_planner_op])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
