from absl import app
from absl import flags

from camera_replay_operator import CameraReplayOperator
from detection_operator import DetectionOperator
from tracker_crt_operator import TrackerCRTOperator
from tracker_cv2_operator import TrackerCV2Operator

import erdos.graph

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_integer('num_cameras', 1, 'Number of cameras.')
flags.DEFINE_bool('obj_detection', False,
                  'True to enable object detection operator')
flags.DEFINE_bool('obj_tracking', False,
                  'True to enable object tracking operator')
flags.DEFINE_string('tracker_type', 'cv2', 'Tracker type: cv2 | crt')


def main(argv):
    graph = erdos.graph.get_current_graph()

    camera_ops = []
    for i in range(0, FLAGS.num_cameras, 1):
        op_name = 'camera{}'.format(i)
        camera_op = graph.add(
            CameraReplayOperator,
            name=op_name,
            setup_args={'op_name': op_name})
        camera_ops.append(camera_op)

    if FLAGS.obj_detection:
        obj_detector_op = graph.add(DetectionOperator, name='detection')
        graph.connect(camera_ops, [obj_detector_op])

        if FLAGS.obj_tracking:
            tracker_op = None
            if FLAGS.tracker_type == 'cv2':
                tracker_op = graph.add(TrackerCV2Operator, name='tracker')
            elif FLAGS.tracker_type == 'crt':
                tracker_op = graph.add(TrackerCRTOperator, name='tracker')
            graph.connect(camera_ops + [obj_detector_op], [tracker_op])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
