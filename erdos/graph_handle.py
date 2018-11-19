from operators import NoopOp


class GraphHandle(object):
    def __init__(self,
                 name,
                 graph_cls,
                 init_args,
                 setup_args,
                 graph_name,
                 parent,
                 framework='ray',
                 machine="",
                 resources=None):
        # Ensure op name uniqueness
        self.name = name if name else "{0}_{1}".format(
            op_cls.__class__.__name__, hash(self))
        assert '/' not in self.name, \
            'Operator name {} contains slashes'.format(self.name)
        self.graph_cls = graph_cls
        self.init_args = init_args if init_args else {}
        self.setup_args = setup_args if setup_args else {}
        self.graph_name = graph_name
        self.parent = parent
        self.framework = framework
        self.machine = machine
        self.resources = {} if resources is None else resources
        self.dependant_ops = []  # handle ids of dependant ops
        self.dependent_op_handles = {}
        self.executor_handle = None
        self.progress_tracker = None  # Unused now
        # Set up input and output ops
        input_op_name = "{}_input_op".format(self.name)
        self.input_op = self.parent.add(NoopOp, name=input_op_name)
        output_op_name = "{}_output_op".format(self.name)
        self.output_op = self.parent.add(NoopOp, name=output_op_name)

    def get_uid(self):
        # TODO(yika): return a better handle than graph_name/op_name
        return "{}/{}".format(self.graph_name, self.name)

    """
    def _build_dependent_op_handles(self, dependent_op_handles):
        for stream in self.output_streams:
            self.dependent_op_handles[stream.uid] = dependent_op_handles.get(
                stream.uid, [])
    """

    def setup_graph(self):
        # TODO(peter) fix this after graphs implement setup_streams
        # Instantiate graph
        graph = self.graph_cls(
            name=self.name,
            parent=self.parent,
            input_op=self.input_op,
            **self.init_args)
        # Construct graph
        output_ops = graph.construct([self.input_op], **self.setup_args)
        # Unroll child subgraphs
        graph._setup_subgraphs()
        # Add operators to parent
        self.parent.op_handles.update(graph.op_handles)
        # Connect output operators to output noop
        self.parent.connect(output_ops, [self.output_op])
        # Set dependent ops of output noop
        self.parent.op_handles[
            self.output_op].dependant_ops += self.dependant_ops
