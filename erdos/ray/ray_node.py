import json
import logging
import ray
import re

from erdos.ray.ray_operator import RayOperator
from erdos.cluster.node import Node

logger = logging.getLogger(__name__)


class RayNode(Node):
    def start_head(self):
        resources = {self.server: 512}
        resources.update(self.total_resources)

        ray_start_command = "ray start --head --resources '{}'".format(
            json.dumps(resources))
        response = self.run_command_sync(ray_start_command)

        matches = re.findall('redis_address="[0-9.:]+"', response)
        if len(matches) != 1:
            raise Exception(
                "Unable to parse redis address:\n{}".format(response))

        redis_address = matches[0].split('"')[1]
        return redis_address

    def setup(self, redis_address):
        resources = {self.server: 512}
        resources.update(self.total_resources)

        ray_start_command = ("ray start --redis-address {} --resources='{}'"
                             .format(self.redis_address,
                                     json.dumps(resources)))
        self.run_command(ray_start_command)

    def teardown(self):
        self.run_command("ray stop")

    def setup_operator(self, op_handle):
        self.op_handles.append(op_handle)

        resources = op_handle.resources.copy() if op_handle.resources else {}
        for k, v in resources.items():
            self.available_resources[k] -= 1
        num_cpus = resources.pop("CPU", None)
        num_gpus = resources.pop("GPU", None)

        # TODO (Yika): hacky solution for using decorator on callbacks
        # When we wrap op in ray operator, __name__ of callbacks that have
        # decorators will turn into "wrapper", so we extract the __name__ here
        for stream in op_handle.input_streams:
            stream.callbacks = set([f.__name__ for f in stream.callbacks])
            stream.completion_callbacks = set(
                [f.__name__ for f in stream.completion_callbacks])
        for stream in op_handle.output_streams:
            stream.callbacks = set([f.__name__ for f in stream.callbacks])
            stream.completion_callbacks = set(
                [f.__name__ for f in stream.completion_callbacks])

        # Create the Ray actor wrapping the ERDOS operator.
        ray_op = RayOperator._remote([op_handle], {}, num_cpus, num_gpus,
                                     resources)
        # Set the actor handle in the ray operator actor.
        ray.get(ray_op.set_handle.remote(ray_op))
        op_handle.executor_handle = ray_op

    def execute_operator(self, op_handle):
        assert op_handle in self.op_handles
        # Setup the input/output streams of the ERDOS operator.
        ray.get(
            op_handle.executor_handle.setup_streams.remote(
                self.op_handle.dependent_op_handles))
        # Start the frequency actor associated to the Ray operator actor.
        # TODO(peter): collocate the the frequency actor on the same machine
        ray.get(self.op_handle.executor_handle.setup_frequency_actor.remote())
        # Execute the operator. We do not call .get here because the executor
        # would block until the operator completes.
        logger.info('Executing {}'.format(self.op_handle.name))
        self.op_handle.executor_handle.execute.remote()
