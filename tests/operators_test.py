from absl import app
from absl import flags

import erdos.graph
from erdos.operators import *
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_bool('where_test', False, 'True to execute the test')
flags.DEFINE_bool('map_test', False, 'True to execute the test')
flags.DEFINE_bool('concat_test', False, 'True to execute the test')
flags.DEFINE_bool('map_many_test', False, 'True to execute the test')
flags.DEFINE_bool('unzip_test', False, 'True to execute the test')


class DataGeneratorOp(Op):
    def __init__(self, name):
        super(DataGeneratorOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        return [
            DataStream(name='data_stream'),
            DataStream(name='tuple_data_stream'),
            DataStream(name='list_data_stream'),
        ]

    @frequency(1)
    def publish_msg(self):
        msg = Message(self._cnt, Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('data_stream').send(msg)
        tuple_msg = Message((self._cnt, 'counter {}'.format(self._cnt)),
                            Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('tuple_data_stream').send(tuple_msg)
        list_msg = Message([self._cnt, self._cnt + 1],
                           Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('list_data_stream').send(list_msg)
        self._cnt += 1

    def execute(self):
        self.publish_msg()


def main(argv):
    graph = erdos.graph.get_current_graph()
    data_gen_op = graph.add(DataGeneratorOp, name='data_gen_op')
    if FLAGS.where_test:
        where_op = graph.add(
            WhereOp,
            name='where_op',
            init_args={
                'where_lambda': lambda msg: msg.data % 2 == 0,
                'output_stream_name': 'where_stream'
            },
            setup_args={
                'filter_stream_lambda': lambda stream: stream.name == 'data_stream',
                'output_stream_name': 'where_stream'
            })
        log_op = graph.add(LogOp, name='where_log_op')
        graph.connect([data_gen_op], [where_op])
        graph.connect([where_op], [log_op])

    if FLAGS.map_test:
        map_op = graph.add(
            MapOp,
            name='map_op',
            init_args={
                'map_lambda': lambda msg: msg.data * msg.data,
                'output_stream_name': 'map_stream'
            },
            setup_args={
                'filter_stream_lambda': lambda stream: stream.name == 'data_stream',
                'output_stream_name': 'map_stream'
            })
        log_op = graph.add(LogOp, name='map_log_op')
        graph.connect([data_gen_op], [map_op])
        graph.connect([map_op], [log_op])

    if FLAGS.concat_test:
        concat_op = graph.add(
            ConcatOp,
            name='concat_op',
            init_args={'output_stream_name': 'concat_stream'},
            setup_args={'output_stream_name': 'concat_stream'})
        log_op = graph.add(LogOp, name='concat_log_op')
        graph.connect([data_gen_op], [concat_op])
        graph.connect([concat_op], [log_op])

    if FLAGS.map_many_test:
        map_many_op = graph.add(
            MapManyOp,
            name='map_many_op',
            init_args={'output_stream_name': 'flat_stream'},
            setup_args={
                'filter_stream_lambda': lambda stream: stream.name == 'list_data_stream',
                'output_stream_name': 'flat_stream'
            })
        log_op = graph.add(LogOp, name='map_many_log_op')
        graph.connect([data_gen_op], [map_many_op])
        graph.connect([map_many_op], [log_op])

    if FLAGS.unzip_test:
        unzip_op = graph.add(
            UnzipOp,
            name='unzip_op',
            init_args={
                'output_stream_name1': 'unzip_left_stream',
                'output_stream_name2': 'unzip_right_stream',
            },
            setup_args={
                'output_stream_name1': 'unzip_left_stream',
                'output_stream_name2': 'unzip_right_stream',
                'filter_stream_lambda': lambda stream: stream.name == 'tuple_data_stream'
            })
        log_op = graph.add(LogOp, name='unzip_log_op')
        graph.connect([data_gen_op], [unzip_op])
        graph.connect([unzip_op], [log_op])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
