import logging
import ray

from erdos.executor import Executor
from erdos.ray.ray_operator import RayOperator

logger = logging.getLogger(__name__)


class RayExecutor(Executor):
    """Helper class to execute Ray operators."""

    def __init__(self, op_handle):
        super(RayExecutor, self).__init__(op_handle)

    def setup(self):
        resources = dict(
            self.op_handle.resources) if self.op_handle.resources else {}
        if self.op_handle.machine:
            resources[self.op_handle.machine] = 1
        num_cpus = self.op_handle.resources.pop("CPU", None)
        num_gpus = self.op_handle.resources.pop("GPU", None)
        # Create the Ray actor wrapping the ERDOS operator.
        ray_op = RayOperator._submit([self.op_handle], {}, num_cpus, num_gpus,
                                     resources)
        # Set the actor handle in the ray operator actor.
        ray.get(ray_op.set_handle.remote(ray_op))
        self.op_handle.executor_handle = ray_op

    def execute(self):
        """Execute Ray operator."""
        # Setup the input/output streams of the ERDOS operator.
        ray.get(
            self.op_handle.executor_handle.setup_streams.remote(
                self.op_handle.dependent_op_handles))
        # Start the frequency actor associated to the Ray operator actor.
        ray.get(self.op_handle.executor_handle.setup_frequency_actor.remote())
        # Execute the operator. We do not call .get here because the executor
        # would block until the operator completes.
        logger.info('Executing {}'.format(self.op_handle.name))
        self.op_handle.executor_handle.execute.remote()
