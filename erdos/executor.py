class Executor(object):
    def __init__(self, op_handle, node):
        self.op_handle = op_handle
        self.node = node

    def setup(self):
        self.node.setup_operator(self.op_handle)

    def execute(self):
        self.node.execte_operator(self.op_handle)
