import json
import re

from erdos.cluster import Cluster


class RayCluster(Cluster):
    def __init__(self, redis_address="", can_install_ray=False):
        super(RayCluster, self).__init__()
        self.redis_address = redis_address
        self.can_install_ray = can_install_ray

    def __del__(self):
        self.broadcast_command("ray stop")
        super(RayCluster, self).__del__()

    def install_ray(self, node):
        node.run_command("pip install ray", timeout=180)

    def ray_installed(self, node):
        return b"not found" not in node.run_command("ray --help")

    def setup_head_node(self, node):
        resources = {node.name: 512}
        ray_start_command = "ray start --head --resources '{}'".format(
            json.dumps(resources))
        response = node.run_command(ray_start_command).decode("utf-8")

        matches = re.findall('redis_address="[0-9.:]+"', response)
        if len(matches) != 1:
            raise Exception("Unable to parse redis address")

        self.redis_address = matches[0].split('"')[1]

    def setup_worker(self, node):
        resources = {node.name: 512}

        ray_start_command = ("ray start --redis-address {} --resources='{}'"
                             .format(self.redis_address,
                                     json.dumps(resources)))
        node.run_command(ray_start_command)

    def setup_node(self, node):
        if not self.ray_installed(node):
            if self.can_install_ray:
                self.install_ray(node)
            else:
                raise Exception("Ray not installed on {}".format(node.server))

        node.run_command("ray stop")

        if not self.redis_address:
            self.setup_head_node(node)
        else:
            self.setup_worker(node)
