import json
import ray
import re

from erdos.cluster.cluster import Cluster
from erdos.local.local_node import LocalNode


class RayCluster(Cluster):
    def __init__(self):
        super(RayCluster, self).__init__()
        self.redis_address = ""

    def __del__(self):
        self.broadcast("ray stop")

    def install_ray(self, node):
        node.run_command("pip install ray", timeout=180)

    def ray_installed(self, node):
        return b"not found" not in node.run_command("ray --help")

    def setup_head_node(self, node):
        self.head_node = node

        resources = {node.server: 512}
        resources.update(**node.total_resources)

        if isinstance(node, LocalNode):
            info = ray.init(resources=resources)
            self.redis_address = info["redis_address"]
        else:
            ray_start_command = "ray start --head --resources '{}'".format(
                json.dumps(resources))
            response = node.run_command_sync(ray_start_command)

            matches = re.findall('redis_address="[0-9.:]+"', response)
            if len(matches) != 1:
                raise Exception("Unable to parse redis address:\n{}".format(response))

            self.redis_address = matches[0].split('"')[1]

    def setup_worker(self, node):
        resources = {node.server: 512}
        resources.update(**node.total_resources)

        if isinstance(node, LocalNode):
            ray.init(redis_address=self.redis_address, resources=resources)
        else:
            ray_start_command = ("ray start --redis-address {} --resources='{}'"
                                 .format(self.redis_address,
                                         json.dumps(resources)))
            node.run_command(ray_start_command)

    def initialize(self):
        assert len(self.nodes) > 0

        self.setup_head_node(self.nodes[0])
        for node in self.nodes[1:]:
            self.setup_worker(node)

        if not ray.is_initialized():
            ray.init(redis_address=self.redis_address)

    def execute(self, node, op_handle):
        raise NotImplementedError
