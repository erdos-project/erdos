from erdos.cluster.ssh_dispatcher import SSHDispatcher


class Node(object):
    """Contains information relevant to a node in a cluster"""

    def __init__(self, server, username, ssh_key, resources=None):
        self.server = server
        self.username = username
        self.ssh_key = ssh_key
        self.total_resources = dict(resources) if resources else dict()
        self.available_resources = dict(self.total_resources)

        self.dispatchers = []
        self.current_dispatcher = self._make_dispatcher()

    def _make_dispatcher(self):
        dispatcher = SSHDispatcher(self.server, self.username, self.ssh_key)
        self.dispatchers.append(dispatcher)
        return dispatcher

    def run_command_sync(self, command, timeout=10):
        """Runs commands and returns command prompt output"""
        self.current_dispatcher.run(command)
        return self.current_dispatcher.wait_for_prompt(
                timeout=timeout).decode("utf-8")

    def run_long_command_asnyc(self, command):
        dispatcher = self._make_dispatcher()
        dispatcher.run(command)
