class OperatorConfig:
    """An :py:class:`OperatorConfig` allows developers to configure an
    operator.

    An operator` can query the configuration passed to it by the driver by
    accessing the properties in :code:`self.config`. The below example shows
    how a `LoggerOperator` can access the log file name passed to the operator
    by the driver::

        class LoggerOperator(erdos.Operator):
            def __init__(self, input_stream):
                # Set up a logger.
                _log = self.config.log_file_name
                self.logger = erdos.utils.setup_logging(self.config.name, _log)
    """

    def __init__(
        self,
        name: str = None,
        flow_watermarks: bool = True,
        log_file_name: str = None,
        csv_log_file_name: str = None,
        profile_file_name: str = None,
    ):
        self._name = name
        self._flow_watermarks = flow_watermarks
        self._log_file_name = log_file_name
        self._csv_log_file_name = csv_log_file_name
        self._profile_file_name = profile_file_name

    @property
    def name(self):
        """Name of the operator."""
        return self._name

    @property
    def flow_watermarks(self):
        """Whether to automatically pass on the low watermark."""
        return self._flow_watermarks

    @property
    def log_file_name(self):
        """File name used for logging."""
        return self._log_file_name

    @property
    def csv_log_file_name(self):
        """File name used for logging to CSV."""
        return self._csv_log_file_name

    @property
    def profile_file_name(self):
        """File named used for profiling an operator's performance."""
        return self._profile_file_name

    def __str__(self):
        return "OperatorConfig(name={}, flow_watermarks={})".format(
            self.name, self.flow_watermarks
        )

    def __repr__(self):
        return str(self)
