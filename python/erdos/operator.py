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
        pass

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
