from absl import app
from absl import flags
from controller_operator import ControllerOperator
from failure_operator import FailureOperator
from source import Source
from sink import Sink

import erdos.graph

FLAGS = flags.FLAGS

flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_string('num_msg', '50',
                    'Total number of messages generated by source')
flags.DEFINE_string('fps', '10',
                    'Source publishing frequency in frames per second')
flags.DEFINE_string('fail_time', '3',
                    'Time elapse in seconds before insert a failure')
flags.DEFINE_string('state_size', '10',
                    'State size (number of integer sequence number) of each operator')
flags.DEFINE_string('checkpoint_freq', '10',
                    'Checkpoint every x number of message/watermarks')


def run_graph(argv):

    # Extract variables
    num_messages = int(FLAGS.num_msg)
    fps = int(FLAGS.fps)
    fail_time = int(FLAGS.fail_time)
    state_size = int(FLAGS.state_size)
    checkpoint_freq = int(FLAGS.checkpoint_freq)

    # Define graph
    graph = erdos.graph.get_current_graph()

    source_op = graph.add(
        Source,
        name='source',
        init_args={'num_messages': num_messages,
                   'fps': fps,
                   'state_size': state_size,
                   'checkpoint_freq': checkpoint_freq}
    )

    failure_op = graph.add(
        FailureOperator,
        name='primary_failure',
        init_args={'state_size': state_size,
                   'checkpoint_freq': checkpoint_freq})

    sink_op = graph.add(
        Sink,
        name='sink',
        init_args={'state_size': state_size,
                   'checkpoint_freq': checkpoint_freq}
    )

    controller_op = graph.add(
        ControllerOperator,
        name='controller',
        init_args={'pre_failure_time_elapse_s': fail_time})

    # Connect Graph
    graph.connect([source_op], [failure_op])
    graph.connect([failure_op], [sink_op])
    graph.connect([sink_op], [controller_op])
    graph.connect([controller_op], [source_op, failure_op, sink_op])

    # Execute Graph
    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(run_graph)