import csv
from absl import app
from absl import flags

import erdos.graph
from erdos.operators import CountWindowTrigger, FileWriterOp, WindowOp, Window, WindowAssigner, WindowProcessor
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_string('front_camera_locations',
                    'front_left,front_center,front_right',
                    'Comma-separated list of locations')


class LogReaderOp(Op):
    def __init__(self, name, output_name, log_file_name):
        super(LogReaderOp, self).__init__(name)
        self._output_name = output_name
        self._log_file_name = log_file_name

    @staticmethod
    def setup_streams(input_streams, output_name):
        return [DataStream(name=output_name)]

    def execute(self):
        output_stream = self.get_output_stream(self._output_name)
        with open(self._log_file_name, 'r') as log_file:
            csv_reader = csv.reader(log_file)
            for row in csv_reader:
                name = row[0]
                processing_time = float(row[1])
                coordinates = [
                    int(val) for val in row[2].strip('[]').split(',')
                ]
                output_msg = Message((name, processing_time),
                                     Timestamp(coordinates=coordinates))
                output_stream.send(output_msg)


class SliddingCountWindowAssigner(WindowAssigner):
    def __init__(self, msg_count):
        self._msg_count = msg_count

    def assign_windows(self, msg):
        window_id = msg.timestamp.coordinates[0]
        return [
            Window(cur_window_id)
            for cur_window_id in range(window_id, window_id + self._msg_count)
        ]


class FrequencyMissProcessor(WindowProcessor):
    def __init__(self, frequency=30):
        super(FrequencyMissProcessor, self).__init__()
        self._inter_time = 1.0 / frequency

    def process(self, msgs):
        assert len(msgs) == 2
        return [Message(msgs[1].data[1] - msgs[0].data[1], msgs[1].timestamp)]


def analyze_frequency(graph, file_name, output_file_name):
    log_reader_op = graph.add(
        LogReaderOp,
        'log_reader',
        init_args={
            'output_name': 'log_reader',
            'log_file_name': file_name
        },
        setup_args={'output_name': 'log_reader'})
    frequency_jitter_op = graph.add(
        WindowOp,
        name='frequency_jitter_op',
        init_args={
            'output_stream_name': 'frequency_jitter_stream',
            'assigner': SliddingCountWindowAssigner(2),
            'trigger': CountWindowTrigger(2),
            'processor': FrequencyMissProcessor()
        },
        setup_args={'output_stream_name': 'frequency_jitter_stream'})
    file_writer_op = graph.add(
        FileWriterOp,
        name='frequency_file_op',
        init_args={'file_name': output_file_name})
    graph.connect([log_reader_op], [frequency_jitter_op])
    graph.connect([frequency_jitter_op], [file_writer_op])


def main(argv):
    graph = erdos.graph.get_current_graph()

    front_locations = FLAGS.front_camera_locations.split(',')

    for location in front_locations:
        analyze_frequency(graph, 'camera_' + location + '.log',
                          'jitter_camera_' + location + '.log')

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
