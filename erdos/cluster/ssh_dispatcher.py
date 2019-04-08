from pexpect import pxssh

from erdos.cluster.dispatcher import Dispatcher


class SSHDispatcher(Dispatcher):
    """Wraps an SSH connection"""
    def __init__(self, server, username, ssh_key):
        super(SSHDispatcher, self).__init__()
        self.connection = pxssh.pxssh()

        if not self.connection.login(server, username, ssh_key=ssh_key):
            raise ValueError("Unable to log into {} with username {}".format(
                server, username))

    def run(self, command):
        self.connection.sendline(command)

    def wait_for_prompt(self, timeout=10):
        self.connection.prompt(timeout=10)
        return self.connection.before

    def read(self, max_bytes=2048):
        return self.connection.read(size=max_bytes)

    def __del__(self):
        self.connection.logout()
