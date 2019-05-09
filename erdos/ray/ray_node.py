import json
import logging
import ray
from absl import flags

from erdos.ray.ray_operator import RayOperator
from erdos.cluster.node import Node

FLAGS = flags.FLAGS

logger = logging.getLogger(__name__)


class RayNode(Node):
    def setup(self, redis_address):
        resources = {self.server: 512}
        resources.update(self.total_resources)

        ray_start_command = "ray start --redis-address {} --resources='{}'"
        if "CPU" in resources:
            num_cpus = resources.pop("CPU")
            ray_start_command += " --num-cpus {}".format(num_cpus)
        if "GPU" in resources:
            num_gpus = resources.pop("GPU")
            ray_start_command += " --num-gpus {}".format(num_gpus)
        ray_start_command = ray_start_command.format(redis_address,
                                                     json.dumps(resources))
        self.run_command_sync(ray_start_command)

    def teardown(self):
        self.run_command_sync("ray stop")

    def setup_operator(self, op_handle):
        self.op_handles.append(op_handle)

        resources = op_handle.resources.copy() if op_handle.resources else {}
        for k, v in resources.items():
            self.available_resources[k] -= 1
        resources[self.server] = 1
        num_cpus = resources.pop("CPU", 0)
        num_gpus = resources.pop("GPU", 0)

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
        op_handle.node = None  # reset node for serialization
        ray_op = RayOperator._remote(
            args=[op_handle],
            kwargs={},
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            resources=resources)
        # Set the actor handle in the ray operator actor.
        ray.get(ray_op.set_handle.remote(ray_op))
        op_handle.executor_handle = ray_op

    def execute_operator(self, op_handle):
        assert op_handle in self.op_handles
        # Setup the input/output streams of the ERDOS operator.
        ray.get(
            op_handle.executor_handle.setup_streams.remote(
                op_handle.dependent_op_handles))
        # Start the frequency actor associated to the Ray operator actor.
        ray.get(op_handle.executor_handle.setup_frequency_actor.remote())
        # Execute the operator. We do not call .get here because the executor
        # would block until the operator completes.
        logger.info('Executing {}'.format(op_handle.name))
        op_handle.executor_handle.execute.remote()


class LocalRayNode(RayNode):
    def __init__(self, resources=None):
        super(LocalRayNode, self).__init__("127.0.0.1", "", "", resources)

    def setup(self):
        """Initializes Ray and returns the redis address"""
        resources = {self.server: 512}
        resources.update(self.total_resources)

        ray_init_kwargs = {}
        if "CPU" in resources:
            num_cpus = resources.pop("CPU")
            ray_init_kwargs["num_cpus"] = num_cpus
        if "GPU" in resources:
            num_gpus = resources.pop("GPU")
            ray_init_kwargs["num_gpus"] = num_gpus
        if FLAGS.ray_redis_address != "":
            ray_init_kwargs["redis_address"] = FLAGS.ray_redis_address

        if not ray.is_initialized():
            info = ray.init(resources=resources, **ray_init_kwargs)
            return info["redis_address"]

    def teardown(self):
        ray.shutdown()

    def _make_dispatcher(self):
        return None
