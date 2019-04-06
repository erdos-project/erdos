from absl import app
from absl import flags

from controller_operator import ControllerOperator
from failure_operator import FailureOperator
from flux_ingress_operator import FluxIngressOperator
from flux_egress_operator import FluxEgressOperator
from flux_consumer_operator import FluxConsumerOperator
from flux_producer_operator import FluxProducerOperator
from source import Source
from sink import Sink

import erdos.graph

FLAGS = flags.FLAGS

flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


def main(argv):
    # Define graph
    graph = erdos.graph.get_current_graph()

    source_op = graph.add(
        Source,
        name='source'
    )

    sink_op = graph.add(
        Sink,
        name='sink'
    )

    sink_op2 = graph.add(
        Sink,
        name='sink2'
    )

    flux_ingress_op = graph.add(
        FluxIngressOperator,
        name='flux_ingress',
        init_args={'output_stream_names': ('primary', 'secondary')},
        setup_args={'output_stream_names': ('primary', 'secondary')})

    graph.connect([source_op], [flux_ingress_op])
    graph.connect([flux_ingress_op], [sink_op, sink_op2])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)