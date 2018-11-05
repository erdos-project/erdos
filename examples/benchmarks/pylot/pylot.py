import os
import sys
import time
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
#from localization_operator import LocalizationOperator
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

import erdos.graph

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


def main(argv):

    graph = erdos.graph.get_current_graph()

    #####################
    #### Create Ops #####
    #####################
    # 8 cameras @ 30 FPS.
    camera_front_left_op = graph.add(
        CameraOperator,
        name='camera_front_left',
        setup_args={'op_name': 'camera_front_left'})
    camera_front_right_op = graph.add(
        CameraOperator,
        name='camera_front_right',
        setup_args={'op_name': 'camera_front_right'})
    camera_front_center_op = graph.add(
        CameraOperator,
        name='camera_front_center',
        setup_args={'op_name': 'camera_front_center'})
    camera_side_left_op = graph.add(
        CameraOperator,
        name='camera_side_left',
        setup_args={'op_name': 'camera_side_left'})
    camera_side_right_op = graph.add(
        CameraOperator,
        name='camera_side_right',
        setup_args={'op_name': 'camera_side_right'})
    camera_rear_left_op = graph.add(
        CameraOperator,
        name='camera_rear_left',
        setup_args={'op_name': 'camera_rear_left'})
    camera_rear_right_op = graph.add(
        CameraOperator,
        name='camera_rear_right',
        setup_args={'op_name': 'camera_rear_right'})
    camera_rear_center_op = graph.add(
        CameraOperator,
        name='camera_rear_center',
        setup_args={'op_name': 'camera_rear_center'})

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
    short_rear_left_radar_op = graph.add(
        RadarOperator,
        name='short_radar_rear_left',
        setup_args={'op_name': 'short_radar_rear_left'})
    short_rear_right_radar_op = graph.add(
        RadarOperator,
        name='short_radar_rear_right',
        setup_args={'op_name': 'short_radar_rear_right'})
    short_front_left_radar_op = graph.add(
        RadarOperator,
        name='short_radar_front_left',
        setup_args={'op_name': 'short_radar_front_left'})
    short_front_right_radar_op = graph.add(
        RadarOperator,
        name='short_radar_front_right',
        setup_args={'op_name': 'short_radar_front_right'})

    # 1 long range radar
    long_radar_op = graph.add(
        RadarOperator, name='long_radar', setup_args={'op_name': 'long_radar'})

    # 3 depth cameras
    depth_camera_front_left_op = graph.add(
        DepthCameraOperator,
        name='depth_camera_front_left',
        setup_args={'op_name': 'depth_camera_front_left'})
    depth_camera_front_right_op = graph.add(
        DepthCameraOperator,
        name='depth_camera_front_right',
        setup_args={'op_name': 'depth_camera_front_right'})
    depth_camera_front_center_op = graph.add(
        DepthCameraOperator,
        name='depth_camera_front_center',
        setup_args={'op_name': 'depth_camera_front_center'})

    # 1 object detector per camera.
    front_left_obj_detector_op = graph.add(
        DetectionOperator,
        name='front_left_obj_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'front_left_obj_det'})

    front_right_obj_detector_op = graph.add(
        DetectionOperator,
        name='front_right_obj_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'front_right_obj_det'})

    front_center_obj_detector_op = graph.add(
        DetectionOperator,
        name='front_center_obj_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'front_center_obj_det'})

    rear_left_obj_detector_op = graph.add(
        DetectionOperator,
        name='rear_left_obj_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'rear_left_obj_det'})

    rear_right_obj_detector_op = graph.add(
        DetectionOperator,
        name='rear_right_obj_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'rear_right_obj_det'})

    rear_center_obj_detector_op = graph.add(
        DetectionOperator,
        name='rear_center_obj_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'rear_center_obj_det'})

    side_left_obj_detector_op = graph.add(
        DetectionOperator,
        name='side_left_obj_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'side_left_obj_det'})

    side_right_obj_detector_op = graph.add(
        DetectionOperator,
        name='side_right_obj_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'side_right_obj_det'})

    # TODO(ionel): Vary number of trackers depending on the number of detected
    # objects.
    front_left_tracker_op = graph.add(
        TrackerOperator,
        name='front_left_tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_left_tracker'})

    front_right_tracker_op = graph.add(
        TrackerOperator,
        name='front_right_tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_right_tracker'})

    front_center_tracker_op = graph.add(
        TrackerOperator,
        name='front_center_tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_center_tracker'})

    rear_left_tracker_op = graph.add(
        TrackerOperator,
        name='rear_left_tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'rear_left_tracker'})

    rear_right_tracker_op = graph.add(
        TrackerOperator,
        name='rear_right_tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'rear_right_tracker'})

    rear_center_tracker_op = graph.add(
        TrackerOperator,
        name='rear_center_tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'rear_center_tracker'})

    side_left_tracker_op = graph.add(
        TrackerOperator,
        name='side_left_tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'side_left_tracker'})

    side_right_tracker_op = graph.add(
        TrackerOperator,
        name='side_right_tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'side_right_tracker'})

    tracker_ops = [
        front_left_tracker_op, front_right_tracker_op, front_center_tracker_op,
        rear_left_tracker_op, rear_right_tracker_op, rear_center_tracker_op,
        side_left_tracker_op, side_right_tracker_op
    ]

    # 3 segmentation operators. One for each front camera.
    front_left_segmentation_op = graph.add(
        SegmentationOperator,
        name='front_left_segmented',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_left_segmented'})

    front_right_segmentation_op = graph.add(
        SegmentationOperator,
        name='front_right_segmented',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_right_segmented'})

    front_center_segmentation_op = graph.add(
        SegmentationOperator,
        name='front_center_segmented',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_center_segmented'})

    # 5 traffic light detectors. One for each front and side camera.
    front_left_traffic_light_det_op = graph.add(
        TrafficLightDetOperator,
        name='front_left_traffic_light',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_left_traffic_light'})

    front_right_traffic_light_det_op = graph.add(
        TrafficLightDetOperator,
        name='front_right_traffic_light',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_right_traffic_light'})

    front_center_traffic_light_det_op = graph.add(
        TrafficLightDetOperator,
        name='front_center_traffic_light',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_center_traffic_light'})

    side_left_traffic_light_det_op = graph.add(
        TrafficLightDetOperator,
        name='side_left_traffic_light',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'side_left_traffic_light'})

    side_right_traffic_light_det_op = graph.add(
        TrafficLightDetOperator,
        name='side_right_traffic_light',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'side_right_traffic_light'})

    # 3 traffic sign detectors. One for each front camera.
    front_left_traffic_sign_det_op = graph.add(
        TrafficSignDetOperator,
        name='front_left_traffic_sign',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_left_traffic_sign'})

    front_right_traffic_sign_det_op = graph.add(
        TrafficSignDetOperator,
        name='front_right_traffic_sign',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_right_traffic_sign'})

    front_center_traffic_sign_det_op = graph.add(
        TrafficSignDetOperator,
        name='front_center_traffic_sign',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_center_traffic_sign'})

    # 5 intersection detectors. One for each front and side camera.
    front_left_intersection_det_op = graph.add(
        IntersectionDetOperator,
        name='front_left_int_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_left_int_det'})

    front_right_intersection_det_op = graph.add(
        IntersectionDetOperator,
        name='front_right_int_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_right_int_det'})

    front_center_intersection_det_op = graph.add(
        IntersectionDetOperator,
        name='front_center_int_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_center_int_det'})

    side_left_intersection_det_op = graph.add(
        IntersectionDetOperator,
        name='side_left_int_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'side_left_int_det'})

    side_right_intersection_det_op = graph.add(
        IntersectionDetOperator,
        name='side_right_int_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'side_right_int_det'})

    # 3 lane detection operatos. One for each front camera.
    front_left_lane_det_op = graph.add(
        LaneDetOperator,
        name='front_left_lane_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_left_lane_det'})

    front_right_lane_det_op = graph.add(
        LaneDetOperator,
        name='front_right_lane_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_right_lane_det'})

    front_center_lane_det_op = graph.add(
        LaneDetOperator,
        name='front_center_lane_det',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'front_center_lane_det'})

    lane_detection_ops = [
        front_left_lane_det_op, front_right_lane_det_op,
        front_center_lane_det_op
    ]

    # 1 SLAM operator.
    slam_op = graph.add(
        SLAMOperator,
        name='SLAM',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        })

    # TODO(ionel): Plugin mapping operator.
    # mapping_op = graph.add(MappingOperator, name='mapping')

    # fusion operator
    fusion_op = graph.add(FusionOperator, name='fusion')

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

    #####################
    #### Connect Ops ####
    #####################
    graph.connect([camera_front_left_op], [
        front_left_obj_detector_op, front_left_segmentation_op,
        front_left_traffic_light_det_op, front_left_traffic_sign_det_op,
        front_left_intersection_det_op, front_left_lane_det_op
    ])
    graph.connect([camera_front_right_op], [
        front_right_obj_detector_op, front_right_segmentation_op,
        front_right_traffic_light_det_op, front_right_traffic_sign_det_op,
        front_right_intersection_det_op, front_right_lane_det_op
    ])
    graph.connect([camera_front_center_op], [
        front_center_obj_detector_op, front_center_segmentation_op,
        front_center_traffic_light_det_op, front_center_traffic_sign_det_op,
        front_center_intersection_det_op, front_center_lane_det_op
    ])
    graph.connect([camera_side_left_op], [
        side_left_obj_detector_op, side_left_traffic_light_det_op,
        side_left_intersection_det_op
    ])
    graph.connect([camera_side_right_op], [
        side_right_obj_detector_op, side_right_traffic_light_det_op,
        side_right_intersection_det_op
    ])
    graph.connect([camera_rear_left_op], [rear_left_obj_detector_op])
    graph.connect([camera_rear_right_op], [rear_right_obj_detector_op])
    graph.connect([camera_rear_center_op], [rear_center_obj_detector_op])

    # Pass object and camera streams to object trackers
    graph.connect([camera_front_left_op, front_left_obj_detector_op],
                  [front_left_tracker_op])
    graph.connect([camera_front_right_op, front_right_obj_detector_op],
                  [front_right_tracker_op])
    graph.connect([camera_front_center_op, front_center_obj_detector_op],
                  [front_center_tracker_op])
    graph.connect([camera_rear_left_op, rear_left_obj_detector_op],
                  [rear_left_tracker_op])
    graph.connect([camera_rear_right_op, rear_right_obj_detector_op],
                  [rear_right_tracker_op])
    graph.connect([camera_rear_center_op, rear_center_obj_detector_op],
                  [rear_center_tracker_op])
    graph.connect([camera_side_left_op, side_left_obj_detector_op],
                  [side_left_tracker_op])
    graph.connect([camera_side_right_op, side_right_obj_detector_op],
                  [side_right_tracker_op])

    graph.connect([lidar_op, long_radar_op, gps_op, imu_op], [slam_op])

    # TODO(ionel): Plug in depth camera streams.

    # TODO(ionel): Plug in short radar streams.

    # TODO(ionel): Connect segmentation operators.

    # TODO(ionel): Plug in intersection, traffic signs, traffic lights, object trackers

    graph.connect([slam_op, lidar_op, gps_op, imu_op] + tracker_ops,
                  [fusion_op])
    graph.connect([fusion_op], [mission_planner_op])
    graph.connect([mission_planner_op, fusion_op] + lane_detection_ops,
                  [motion_planner_op])

    #####################
    ### Execute Graph ###
    #####################
    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
