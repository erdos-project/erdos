from cv_bridge import CvBridge
import cv2
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import tensorflow as tf
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from detection_utils import add_bounding_box, load_coco_labels


class DetectionOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 model_path,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(DetectionOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._last_seq_num = -1
        self._output_stream_name = output_stream_name
        self._bridge = CvBridge()
        self._detection_graph = tf.Graph()
        with self._detection_graph.as_default():
            od_graph_def = tf.GraphDef()
            with tf.gfile.GFile(model_path, 'rb') as fid:
                serialized_graph = fid.read()
                od_graph_def.ParseFromString(serialized_graph)
                tf.import_graph_def(od_graph_def, name='')

        self._gpu_options = tf.GPUOptions(
            per_process_gpu_memory_fraction=flags.obj_detection_gpu_memory_fraction)
        self._tf_session = tf.Session(
            graph=self._detection_graph,
            config=tf.ConfigProto(gpu_options=self._gpu_options))
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
        self._coco_labels = load_coco_labels(self._flags.path_coco_labels)

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        input_streams.add_callback(DetectionOperator.on_msg_camera_stream)
        # TODO(Ionel): specify data type here
        return [DataStream(name=output_stream_name,
                           labels={'obstacles': 'true'})]

    def on_msg_camera_stream(self, msg):
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

        self._logger.info('{} received frame {}'.format(self.name, msg.timestamp))
        start_time = time.time()
        image_np = self._bridge.imgmsg_to_cv2(msg.data, 'rgb8')
        # Expand dimensions since the model expects images to have
        # shape: [1, None, None, 3]
        image_np_expanded = np.expand_dims(image_np, axis=0)
        (boxes, scores, classes, num_detections) = self._tf_session.run(
            [
                self._detection_boxes, self._detection_scores,
                self._detection_classes, self._num_detections
            ],
            feed_dict={self._image_tensor: image_np_expanded})

        num_detections = int(num_detections[0])
        classes = classes[0][:num_detections]
        labels = [self._coco_labels[label] for label in classes]
        boxes = boxes[0][:num_detections]
        scores = scores[0][:num_detections]

        self._logger.info('Object boxes {}'.format(boxes))
        self._logger.info('Object scores {}'.format(scores))
        self._logger.info('Object labels {}'.format(labels))

        img = Image.fromarray(np.uint8(image_np)).convert('RGB')

        index = 0
        output = []
        im_width, im_height = img.size
        while index < len(boxes) and index < len(scores):
            if scores[index] > self._flags.detector_min_score_threshold:
                ymin = int(boxes[index][0] * im_height)
                xmin = int(boxes[index][1] * im_width)
                ymax = int(boxes[index][2] * im_height)
                xmax = int(boxes[index][3] * im_width)
                corners = (xmin, xmax, ymin, ymax)
                output.append((corners, scores[index], labels[index]))
                add_bounding_box(img, corners)
            index += 1

        if self._flags.visualize_detector_output:
            draw = ImageDraw.Draw(img)
            draw.text((5, 5),
                      "Timestamp: {}".format(msg.timestamp),
                      fill='black')
            open_cv_image = np.array(img)
            open_cv_image = open_cv_image[:, :, ::-1].copy()
            cv2.imshow(self.name, open_cv_image)
            cv2.waitKey(1)

        # Get runtime in ms.
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},"{}",{}'.format(
            time_epoch_ms(), self.name, msg.timestamp, runtime))
        output_msg = Message((output, runtime), msg.timestamp)
        self.get_output_stream(self._output_stream_name).send(output_msg)

    def execute(self):
        self.spin()
