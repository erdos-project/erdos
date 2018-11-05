from absl import app
from absl import flags

import erdos.graph
from input_operator import InputOperator
from synchronizer_last_data_operator import SynchronizerLastDataOperator
from synchronizer_best_data_operator import SynchronizerBestDataOperator
from synchronizer_data_within_limit_operator import SynchronizerDataWithinLimitOperator

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


def main(argv):
    graph = erdos.graph.get_current_graph()

    input_ops = []
    for i in range(0, 2):
        input_op = graph.add(InputOperator, name='input' + str(i))
        input_ops.append(input_op)

    sync_op = graph.add(SynchronizerLastDataOperator, name='synchronizer')
    # sync_op = SynchronizerBestDataOperator()
    # sync_op = SynchronizerDataWithinLimitOperator(500)

    # TODO(ionel): Implement!
    # sync_op = SynchronizerByDeadlineOperator()
    # sync_op = SynchronizerBestDataWithDelay()
    graph.connect(input_ops, [sync_op])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
