import os
import sys
from absl import app
from absl import flags

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from examples.benchmarks.pylot.camera_operator import CameraOperator
from examples.benchmarks.pylot.lidar_operator import LidarOperator
from examples.benchmarks.pylot.detection_operator import DetectionOperator
from examples.benchmarks.pylot.segmentation_operator import SegmentationOperator
from examples.benchmarks.pylot.slam_operator import SLAMOperator
from examples.benchmarks.pylot.tracker_operator import TrackerOperator

import erdos.graph

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


def main(argv):

    # Set up graph
    graph = erdos.graph.get_current_graph()

    # Add operators
    camera = graph.add(
        CameraOperator, name='camera', setup_args={'op_name': 'camera'})
    detector = graph.add(
        DetectionOperator,
        name='detector',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100,
            'min_det_objs': 3,
            'max_det_objs': 15
        },
        setup_args={'op_name': 'detector'})
    lidar = graph.add(
        LidarOperator,
        name='lidar',
        init_args={'num_points': 100000},
        setup_args={'op_name': 'lidar'})
    tracker = graph.add(
        TrackerOperator,
        name='tracker',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'tracker'})
    segmentation = graph.add(
        SegmentationOperator,
        name='seg',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        },
        setup_args={'op_name': 'seg'})
    slam = graph.add(
        SLAMOperator,
        name='SLAM',
        init_args={
            'min_runtime_us': 1,
            'max_runtime_us': 100
        })

    # Connect operators
    graph.connect([camera], [detector, segmentation])
    graph.connect([camera, detector], [tracker])
    graph.connect([lidar, tracker], [slam])

    # Execute graph
    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
