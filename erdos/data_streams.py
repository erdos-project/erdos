class DataStreams(object):
    def __init__(self, streams):
        self._streams = streams

    def filter(self, filter_func):
        """General filter function on data streams.

        Args:
            filter_func (list of DataStream -> list of DataStream): filters
                the data streams arbitrarily.

        Returns:
            (DataStreams): filtered data streams.
        """
        if filter_func:
            result = filter(filter_func, self._streams)
            return DataStreams(result)
        else:
            return self

    def filter_name(self, name):
        """Filter on `stream.name`.

        Args:
            name (str, list of str): either a string or a list of strings
                describing stream names.

        Returns:
            (DataStreams): filtered data streams.
        """
        if type(name) == str:
            result = [
                stream for stream in self._streams if stream.name == name
            ]
        elif type(name) == list:
            result = [
                stream for stream in self._streams if stream.name in name
            ]
        else:
            raise TypeError(
                "filter_name function takes in either str or list of str type")
        return DataStreams(result)

    def at_least(self, n):
        assert len(self._streams) >= n
        return self

    def at_most(self, n):
        assert len(self._streams) <= n
        return self

    def exact(self, n):
        assert len(self._streams) == n
        return self

    def between(self, min_n, max_n):
        assert min_n <= len(self._streams) <= max_n
        return self

    def add_callback(self, callback_func):
        """Registers a callback function on all data streams.

        Returns:
            (DataStreams): selected data streams.
        """
        for stream in self._streams:
            stream.add_callback(callback_func)
        return self
