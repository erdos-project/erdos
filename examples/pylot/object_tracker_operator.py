from erdos.op import Op


class ObjectTrackerOperator(Op):
    def __init__(self, name='motion-planner'):
        super(ObjectTrackerOperator, self).__init__(name)
        self._prev_image = None

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(ObjectTrackerOperator.on_objects_msg)
        return []

    def on_objects_msg(self, msg):
        # TODO(ionel): Implement!
        (image, objects) = msg.data
        self._prev_image = image

    def execute(self):
        self.spin()
