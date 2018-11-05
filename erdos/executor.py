class Executor(object):
    def __init__(self, op_handle):
        self.op_handle = op_handle

    def setup(self):
        pass

    def execute(self):
        raise NotImplementedError('Each executor must define execute.')
