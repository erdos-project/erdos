import cv2

from carla.image_converter import labels_to_cityscapes_palette

from erdos.op import Op


class SegmentedVideoOperator(Op):
    def __init__(self, name):
        super(SegmentedVideoOperator, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams, filter):
        input_streams.filter_name(filter)\
            .add_callback(SegmentedVideoOperator.display_frame)
        return []

    def display_frame(self, msg):
        frame_array = labels_to_cityscapes_palette(msg.data)
        cv2.imshow(self.name, frame_array)
        cv2.waitKey(1)

    def execute(self):
        self.spin()
