from absl import flags
from cv_bridge import CvBridge
import cv2
import numpy as np
import tensorflow as tf
import PIL.Image as Image

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

from utils import add_bounding_box

FLAGS = flags.FLAGS
flags.DEFINE_float('traffic_light_det_min_score_threshold', 0.3,
                   'Min score threshold for bounding box')
flags.DEFINE_bool('visualize_traffic_light_output', False,
                  'True to enable visualization of traffic light output')


class TrafficLightDetOperator(Op):
    def __init__(self, name, output_stream_name, model_path):
        super(TrafficLightDetOperator, self).__init__(name)
        self._logger = setup_logging(self.name, FLAGS.log_file_name)
        self._output_stream_name = output_stream_name
        self._bridge = CvBridge()
        self._detection_graph = tf.Graph()
        with self._detection_graph.as_default():
            od_graph_def = tf.GraphDef()
            with tf.gfile.GFile(model_path, 'rb') as fid:
                serialized_graph = fid.read()
                od_graph_def.ParseFromString(serialized_graph)
                tf.import_graph_def(od_graph_def, name='')

        self._tf_session = tf.Session(graph=self._detection_graph)
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
        self._labels = {
            1: 'Green',
            2: 'Red',
            3: 'Yellow',
            4: 'Off'
        }

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        input_streams.add_callback(TrafficLightDetOperator.on_frame)
        # TODO(ionel): Specify output stream type
        return [DataStream(name=output_stream_name,
                           labels={'traffic_lights': 'true'})]

    def on_frame(self, msg):
        image_np = self._bridge.imgmsg_to_cv2(msg.data, 'rgb8')
        # Expand dimensions since the model expects images to have shape: [1, None, None, 3]
        image_np_expanded = np.expand_dims(image_np, axis=0)
        (boxes, scores, classes, num) = self._tf_session.run(
            [
                self._detection_boxes, self._detection_scores,
                self._detection_classes, self._num_detections
            ],
            feed_dict={self._image_tensor: image_np_expanded})

        num_detections = int(num[0])
        labels = [self._labels[label]
                  for label in classes[0][:num_detections]]
        boxes = boxes[0][:num_detections]
        scores = scores[0][:num_detections]
        
        self._logger.info('Traffic light boxes {}'.format(boxes))
        self._logger.info('Traffic light scores {}'.format(scores))
        self._logger.info('Traffic light labels {}'.format(labels))
        
        img = Image.fromarray(np.uint8(image_np)).convert('RGB')

        index = 0
        output = []
        im_width, im_height = img.size
        while index < len(boxes) and index < len(scores):
            if scores[index] > FLAGS.traffic_light_det_min_score_threshold:
                ymin = int(boxes[index][0] * im_height)
                xmin = int(boxes[index][1] * im_width)
                ymax = int(boxes[index][2] * im_height)
                xmax = int(boxes[index][3] * im_width)
                corners = (xmin, xmax, ymin, ymax)
                output.append((corners, scores[index], labels[index]))
                add_bounding_box(img, corners)
            index += 1

        if FLAGS.visualize_traffic_light_output:
            open_cv_image = np.array(img)
            open_cv_image = open_cv_image[:, :, ::-1].copy()
            cv2.imshow(self.name, open_cv_image)
            cv2.waitKey(1)

        output_msg = Message(output, msg.timestamp)
        self.get_output_stream(self._output_stream_name).send(output_msg)

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.spin()
        self._tf_session.close()
