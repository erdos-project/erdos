import pexpect
import time

from erdos.cluster.dispatcher import Dispatcher

class LocalDispatcher(Dispatcher):
    """Run commands locally"""
    def __init__(self):
        super(LocalDispatcher, self).__init__()
        self.current_process = None
        self.processes = []

    def run(self, command):
        self.current_process = pexpect.spawn(command)
        self.processes.append(self.current_process)

    def wait_for_prompt(self, timeout=10):
        if self.current_process is None:
            return b""

        start = time.time()
        end = start + timeout
        while time.time() < end:
            if not self.current_process.isalive():
                process = self.current_process
                self.current_process = None
                self._cleanup_processes()
                return process.read()

        raise Exception("Timeout while waiting for prompt")

    def _cleanup_processes(self):
        self.processes = list(filter(lambda p: p.isalive(), self.processes))
