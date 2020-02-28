class Operator(object):
    """Operator abstract base class.

    Inherit from this class when creating an operator.
    """
    def __init__(self, *streams):
        """Instantiates the operator.

        ERDOS will pass read streams followed by write streams as arguments,
        matching the read streams and write streams in `connect()`.

        Invoked automatically during `erdos.run()`.
        """
        self._trace_events = []

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

    def save_trace_events(self, file_name):
        import json
        with open(file_name, "w") as write_file:
            json.dump(self._trace_events, write_file)

    def _add_trace_event(self, event):
        self._trace_events.append(event)
