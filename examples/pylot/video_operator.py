import cv2

from carla.image_converter import to_bgra_array

from erdos.op import Op


class VideoOperator(Op):
    def __init__(self, name):
        super(VideoOperator, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams, filter_name=None):
        if filter_name:
            input_streams = input_streams.filter_name(filter_name)
        input_streams.add_callback(VideoOperator.display_frame)
        return []

    def display_frame(self, msg):
        frame_array = to_bgra_array(msg.data)
        cv2.imshow(self.name, frame_array)
        cv2.waitKey(1)

    def execute(self):
        self.spin()
