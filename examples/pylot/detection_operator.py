from cv_bridge import CvBridge
import numpy as np
import tensorflow as tf
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import PIL.ImageFont as ImageFont


from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

MODEL_PATH = 'dependencies/ssd_mobilenet_v1_coco_2018_01_28/frozen_inference_graph.pb'
#MODEL_PATH = 'dependencies/faster_rcnn_resnet101_coco_2018_01_28/frozen_inference_graph.pb'


class DetectionOperator(Op):
    def __init__(self, name):
        super(DetectionOperator, self).__init__(name)
        self._logger = setup_logging(self.name)
        self._tf_session = None
        self._bridge = CvBridge()
        self._min_score_threshold = 0.3
        self._detection_graph = tf.Graph()
        with self._detection_graph.as_default():
            od_graph_def = tf.GraphDef()
            with tf.gfile.GFile(MODEL_PATH, 'rb') as fid:
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

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(DetectionOperator.on_msg_camera_stream)
        # TODO(Ionel): specify data type here
        return [DataStream(name='obj_stream')]

    def add_bounding_box_text(self, draw, xmin, ymax, ymin, color, texts):
        try:
            font = ImageFont.truetype('arial.ttf', 24)
        except IOError:
            font = ImageFont.load_default()
        # Check if there's space to add text at the top of the box.
        text_str_heights = [font.getsize(text)[1] for text in texts]
        # Each display_str has a top and bottom margin of 0.05x.
        total_text_str_height = (1 + 2 * 0.05) * sum(text_str_heights)

        if ymax > total_text_str_height:
            text_bottom = ymax
        else:
            text_bottom = ymin + total_text_str_height
        # Reverse list and print from bottom to top.
        for text in texts[::-1]:
            text_width, text_height = font.getsize(text)
            margin = np.ceil(0.05 * text_height)
            draw.rectangle(
                [(xmin, text_bottom - text_height - 2 * margin),
                 (xmin + text_width, text_bottom)],
                fill=color)
            draw.text(
                (xmin + margin, text_bottom - text_height - margin),
                text, fill='black', font=font)
            text_bottom -= text_height - 2 * margin

    def add_bounding_box(self, image, corners, color='red',
                         thickness=4, texts=None):
        draw = ImageDraw.Draw(image)
        (xmin, xmax, ymin, ymax) = corners
        draw.line([(xmin, ymax), (xmin, ymin), (xmax, ymin),
                   (xmax, ymax), (xmin, ymax)], width=thickness, fill=color)
        if texts:
            self.add_bounding_box_text(draw, xmin, ymax, ymin, color, texts)

    def on_msg_camera_stream(self, msg):
        self._logger.info('%s received frame %s', self.name, msg.timestamp)
        image_np = self._bridge.imgmsg_to_cv2(msg.data, "bgr8")
        # Expand dimensions since the model expects images to have shape: [1, None, None, 3]
        image_np_expanded = np.expand_dims(image_np, axis=0)
        (boxes, scores, detection_classes, num_detections) = self._tf_session.run(
            [
                self._detection_boxes, self._detection_scores,
                self._detection_classes, self._num_detections
            ],
            feed_dict={self._image_tensor: image_np_expanded})

        num_detections = int(num_detections[0])
        #classes = detection_classes[0].astype(np.uint8)
        boxes = boxes[0][:num_detections]
        scores = scores[0][:num_detections]

        self._logger.info('Object boxes {}'.format(boxes))
        self._logger.info('Object scores {}'.format(scores))

        img = Image.fromarray(np.uint8(image_np)).convert('RGB')

        index = 0
        output_boxes = []
        im_width, im_height = img.size
        while index < len(boxes) and index < len(scores):
            if scores[index] > self._min_score_threshold:
                ymin = boxes[index][0] * im_height
                xmin = boxes[index][1] * im_width
                ymax = boxes[index][2] * im_height
                xmax = boxes[index][3] * im_width
                corners = (xmin, xmax, ymin, ymax)
                output_boxes.append(corners)
                self.add_bounding_box(img, corners)
            index += 1

        # img.show()

        output_msg = Message(output_boxes, msg.timestamp)
        self.get_output_stream('obj_stream').send(output_msg)

    def execute(self):
        self.spin()
        self._tf_session.close()
