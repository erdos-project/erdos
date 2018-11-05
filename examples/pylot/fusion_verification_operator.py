from collections import deque
import numpy as np

import erdos
from erdos.op import Op
from erdos.utils import setup_logging


class FusionVerificationOperator(Op):
    def __init__(self, name):
        super(FusionVerificationOperator, self).__init__(name)
        self.vehicles = deque()
        self._logger = setup_logging(self.name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter_name('vehicles').add_callback(
            FusionVerificationOperator.on_vehicles_update)
        input_streams.filter_name('fusion_vehicles').add_callback(
            FusionVerificationOperator.on_fusion_update)
        return []

    def on_vehicles_update(self, msg):
        vehicle_positions = []
        for vehicle in msg.data:
            position = np.array(
                [vehicle.position.location[0], vehicle.position.location[1]])
            vehicle_positions.append(position)

        self.vehicles.append((msg.timestamp, vehicle_positions))

    def on_fusion_update(self, msg):
        while self.vehicles[0][0] < msg.timestamp:
            self.vehicles.popleft()

        predictions = msg.data
        truths = self.vehicles[0][1]
        min_errors = []
        for prediction in predictions:
            min_error = float("inf")
            for truth in truths:
                error = np.linalg.norm(prediction - truth)
                min_error = min(error, min_error)
            min_errors.append(min_error)

        self._logger.info(
            "Fusion: min vehicle position errors: {}".format(min_errors))

    def execute(self):
        self.spin()
