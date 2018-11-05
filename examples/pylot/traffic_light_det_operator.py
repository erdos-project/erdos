import cv2
from cv_bridge import CvBridge
import numpy as np
import tensorflow as tf

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

FASTER_RCNN_SIM_MODEL = 'dependencies/data/traffic_light_det_inference_graph.pb'


class TrafficLightDetOperator(Op):
    def __init__(self, name):
        super(TrafficLightDetOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._bridge = None
        self._tf_sesion = None

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(TrafficLightDetOperator.on_frame)
        # TODO(ionel): Specify output stream type
        return [DataStream(name='traffic_lights')]

    def on_frame(self, msg):
        image_np = self._bridge.imgmsg_to_cv2(msg.data, "bgr8")
        # Expand dimensions since the model expects images to have shape: [1, None, None, 3]
        image_np_expanded = np.expand_dims(image_np, axis=0)
        (boxes, scores, classes, num) = self._tf_sesion.run(
            [
                self._detection_boxes, self._detection_scores,
                self._detection_classes, self._num_detections
            ],
            feed_dict={self._image_tensor: image_np_expanded})
        self._logger.info('Traffic light boxes {}'.format(boxes))
        self._logger.info('Traffic light scores {}'.format(scores))
        output_msg = Message((boxes, scores, classes), msg.timestamp)
        self.get_output_stream('traffic_lights').send(output_msg)

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self._bridge = CvBridge()
        self._detection_graph = tf.Graph()
        with self._detection_graph.as_default():
            od_graph_def = tf.GraphDef()
            with tf.gfile.GFile(FASTER_RCNN_SIM_MODEL, 'rb') as fid:
                serialized_graph = fid.read()
                od_graph_def.ParseFromString(serialized_graph)
                tf.import_graph_def(od_graph_def, name='')

        self._tf_sesion = tf.Session(graph=self._detection_graph)
        self._image_tensor = self._detection_graph.get_tensor_by_name(
            'image_tensor:0')
        self._detection_boxes = self._detection_graph.get_tensor_by_name(
            'detection_boxes:0')
        self._detection_scores = self._detection_graph.get_tensor_by_name(
            'detection_scores:0')
        self._detection_classes = self._detection_graph.get_tensor_by_name(
            'detection_classes:0')
        self._num_detections = self._detection_graph.get_tensor_by_name(
            'num_detections:0')

        self.spin()
        self._tf_sesion.close()
