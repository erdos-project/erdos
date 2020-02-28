class Operator(object):
    """Operator abstract base class.

    Inherit from this class when creating an operator.
    """
    def __init__(self, *streams):
        """Instantiates the operator.

        ERDOS will pass read streams followed by write streams as arguments,
        matching the read streams and write streams in `connect()`.

        Invoked automatically during `erdos.run()`.

        ERDOS operators never need to call super().__init__(*streams) because
        setup is handled by the ERDOS backend code.
        """
        pass

    def __new__(cls, *args, **kwargs):
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(Operator, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        return instance

    @staticmethod
    def connect(*read_streams):
        """Connects the operator to its read streams and returns its write streams.

        This method should return all write streams it intends to use.

        Invoked automatically during `erdos.connect`.
        """
        raise NotImplementedError

    def run(self):
        """Runs the operator.

        Invoked automaticaly during `erdos.run()`.
        """
        pass

    @property
    def name(self):
        """Returns the operator's name."""
        return self._name

    @property
    def id(self):
        """Returns the operator's ID."""
        return self._id

    def save_trace_events(self, file_name):
        import json
        with open(file_name, "w") as write_file:
            json.dump(self._trace_events, write_file)

    def _add_trace_event(self, event):
        self._trace_events.append(event)
