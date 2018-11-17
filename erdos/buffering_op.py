try:
    import queue as queue
except ImportError:
    import Queue as queue

from erdos.op import Op


class BufferingOp(Op):
    def __init__(self, name):
        super(BufferingOp, self).__init__(name)
        self._event_log_buffer = queue.Queue()

    def __del__(self):
        self.flush()

    def flush(self):
        with open('{}.log'.format(self.name), 'a+') as output_file:
            while not self._event_log_buffer.empty():
                event_log = self._event_log_buffer.get()
                output_file.write(event_log)
        self._event_log_buffer = []

    def log_event(self, processing_time, timestamp, log_message=None):
        self._event_log_buffer.put((self.name, processing_time, timestamp,
                                    log_message))
