from absl import app
from absl import flags

from controller_operator import ControllerOperator
from failure_operator import FailureOperator
from flux_ingress_operator import FluxIngressOperator
from flux_egress_operator import FluxEgressOperator
from flux_consumer_operator import FluxConsumerOperator
from flux_producer_operator import FluxProducerOperator

import erdos.graph

FLAGS = flags.FLAGS

flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


def main(argv):

    # Define graph
    graph = erdos.graph.get_current_graph()

    flux_ingress_op = graph.add(
        FluxIngressOperator,
        name='flux_ingress',
        init_args={'primary_stream_name': 'primary_consumer',
                   'secondary_stream_name': 'secondary_consumer',
                   'primary_ack_stream_name': 'primary_ack',
                   'secondary_ack_stream_name': 'secondary_ack'},
        setup_args={'primary_stream_name': 'primary_consumer',
                    'secondary_stream_name': 'secondary_consumer'})

    flux_primary_consumer_op = graph.add(
        FluxConsumerOperator,
        name='flux_primary_consumer',
        init_args={'primary': True,
                   'output_stream_name': 'primary_data_stream',
                   'ack_stream_name': 'primary_ack'},
        setup_args={'output_stream_name': 'primary_data_stream',
                    'ack_stream_name': 'primary_ack'})
    flux_secondary_consumer_op = graph.add(
        FluxConsumerOperator,
        name='flux_secondary_consumer',
        init_args={'primary': False,
                   'output_stream_name': 'secondary_data_stream',
                   'ack_stream_name': 'secondary_ack'},
        setup_args={'output_stream_name': 'secondary_data_stream',
                    'ack_stream_name': 'secondary_ack'})

    flux_primary_producer_op = graph.add(
        FluxProducerOperator,
        name='flux_primary_producer',
        init_args={'primary': True,
                   'output_stream_name': 'primary_producer'},
        setup_args={'output_stream_name': 'primary_producer'})
    flux_secondary_producer_op = graph.add(
        FluxProducerOperator,
        name='flux_secondary_producer',
        init_args={'primary': False,
                   'output_stream_name': 'secondary_producer'},
        setup_args={'output_stream_name': 'secondary_producer'})

    flux_egress_op = graph.add(
        FluxEgressOperator,
        name='flux_egress',
        init_args={'output_stream_name': 'output_stream',
                   'ack_stream_name': 'ergress_ack'},
        setup_args={'output_stream_name': 'output_stream',
                    'ack_stream_name': 'ergress_ack'})

    primary_failure_op = graph.add(
        FailureOperator,
        name='primary_failure',
        init_args={'output_stream_name': 'primary_failure',
                   'primary': True},
        setup_args={'output_stream_name': 'primary_failure'})

    secondary_failure_op = graph.add(
        FailureOperator,
        name='secondary_failure',
        init_args={'output_stream_name': 'secondary_failure',
                   'primary': False},
        setup_args={'output_stream_name': 'secondary_failure'})

    # NOTE: The controller generates failure messages and input data.
    controller_op = graph.add(
        ControllerOperator,
        name='controller')

    # Connect the controller op to the operators that need to receive node
    # failure messages.
    graph.connect([controller_op],
                  [flux_ingress_op, flux_primary_producer_op,
                   flux_secondary_producer_op, flux_egress_op] +
                  [primary_failure_op, secondary_failure_op])
    
    # Connect the ingress operator to the consumer replicas.
    graph.connect([flux_ingress_op],
                  [flux_primary_consumer_op, flux_secondary_consumer_op])
    # Connect the consumer replicas to the ingress operator so that it can
    # listen on ACK streams.
    graph.connect([flux_primary_consumer_op, flux_secondary_consumer_op],
                  [flux_ingress_op])

    graph.connect([flux_primary_consumer_op], [primary_failure_op])
    graph.connect([flux_secondary_consumer_op], [secondary_failure_op])

    graph.connect([primary_failure_op], [flux_primary_producer_op])
    graph.connect([secondary_failure_op], [flux_secondary_producer_op])

    # Connect the primary operators to the egress operator.
    graph.connect([flux_primary_producer_op, flux_secondary_producer_op],
                  [flux_egress_op])
    # Connect the egress operator to the secondary; so that it can ACK
    # messages.
    graph.connect([flux_egress_op], [flux_secondary_producer_op])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)

