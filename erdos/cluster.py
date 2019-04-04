from pexpect import pxssh


class Cluster(object):
    """Cluster base class.

    All clusters must inherent from this class, and must implement:

    1. setup_node: Runs commands to connect the node to the cluster.
    """

    def __init__(self):
        self.nodes = {}

    def __del__(self):
        for node in self.nodes.values():
            node.disconnect()

    def add_node(self,
                 name,
                 server,
                 username,
                 ssh_key,
                 setup_env_commands=None):
        """Adds a node to the cluster and sets it up"""
        node = Node(name, server, username, ssh_key)
        self.nodes[name] = node

        if setup_env_commands is not None:
            for command in setup_env_commands:
                node.run_command(command)

        self.setup_node(node)

        return node

    def setup_node(self, node):
        pass

    def get_node(self, name):
        return self.node[name]

    def broadcast_command(self, command):
        for node in self.nodes.values():
            node.run_command_async(command)


class Node(object):
    """Wraps an SSH connection to a node"""

    def __init__(self, name, server, username, ssh_key):
        self.name = name
        self.server = server
        self.username = username
        self.ssh_key = ssh_key
        self.connection = pxssh.pxssh()

        self.connect()

    def connect(self):
        """Creates an SSH connection to the node.

        This is called by default in __init__.
        """
        if not self.connection.login(
                self.server, self.username, ssh_key=self.ssh_key):
            raise Exception("Unable to log into {} with username {}".format(
                self.server, self.username))

    def disconnect(self):
        self.connection.logout()

    def run_command(self, command, timeout=-1):
        """Runs commands and returns command prompt output"""
        self.run_command_async(command)
        if not self.connection.prompt(timeout=timeout):
            raise Exception("Lost SSH connection to {}".format(self.server))

        return self.connection.before

    def run_command_async(self, command):
        self.connection.sendline(command)
