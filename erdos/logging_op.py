try:
    import queue as queue
except ImportError:
    import Queue as queue

from erdos.op import Op


class LoggingOp(Op):
    def __init__(self, name, buffer_logs=False):
        super(LoggingOp, self).__init__(name)
        self._buffer_logs = buffer_logs
        self._event_log_buffer = queue.Queue()
        self._log_file = open('{}.log'.format(self.name), 'a+')

    def __del__(self):
        self.flush()
        self._log_file.close()

    def flush(self):
        while not self._event_log_buffer.empty():
            (name, processing_time, timestamp,
             log_message) = self._event_log_buffer.get()
            self._log_file.write(name + ',' + str(processing_time) + ',' +
                                 str(timestamp) + ',' + str(log_message) +
                                 '\n')

    def log_event(self, processing_time, timestamp, log_message=None):
        if self._buffer_logs:
            self._event_log_buffer.put((self.name, processing_time, timestamp,
                                        log_message))
        else:
            self._log_file.write(self.name + ',' + str(processing_time) + ',' +
                                 str(timestamp) + ',' + str(log_message) +
                                 '\n')
