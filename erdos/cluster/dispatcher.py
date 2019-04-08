class Dispatcher(object):
    """Dispatcher base class.
    
    All dispatchers must inherit from this class, and must implement:

    1. run: Runs a command.
    2. wait_for_prompt: Waits for the next prompt is available and returns all
        output from the previous command.
    3. read: Reads output.
    """
    def run(self, command):
        raise NotImplementedError

    def wait_for_prompt(self, timeout=10):
        raise NotImplementedError

    def read(self, max_bytes=2048):
        raise NotImplementedError
