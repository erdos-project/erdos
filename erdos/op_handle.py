class OpHandle(object):
    def __init__(self,
                 name,
                 op_cls,
                 init_args,
                 setup_args,
                 graph_name,
                 framework='ray',
                 machine="",
                 resources=None):
        # Ensure op name uniqueness
        self.name = name if name else "{0}_{1}".format(
            op_cls.__class__.__name__, hash(self))
        assert '/' not in self.name, \
            'Operator name {} contains slashes'.format(self.name)
        self.op_cls = op_cls
        self.init_args = init_args if init_args else {}
        self.setup_args = setup_args if setup_args else {}
        self.graph_name = graph_name
        self.driver_input_streams = []
        self.input_streams = []
        self.output_streams = []
        self.framework = framework
        self.machine = machine
        self.resources = {} if resources is None else resources
        self.dependant_ops = []  # handle ids of dependant ops
        self.dependent_op_handles = {}
        self.executor_handle = None
        self.progress_tracker = None  # Unused now

    def get_uid(self):
        # TODO(yika): return a better handle than graph_name/op_name
        return "{}/{}".format(self.graph_name, self.name)

    def _build_dependent_op_handles(self, dependent_op_handles):
        for stream in self.output_streams:
            self.dependent_op_handles[stream.uid] = dependent_op_handles.get(
                stream.uid, [])
