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

    flux_ingress_op = graph.add(
        FluxIngressOperator,
        name='flux_ingress',
        init_args={'output_stream_names': ('primary', 'secondary')},
        setup_args={'output_stream_names': ('primary', 'secondary')})

    flux_primary_consumer_op = graph.add(
        FluxConsumerOperator,
        name='flux_primary_consumer',
        init_args={'replica_num': 0,
                   'output_stream_name': 'primary_data_stream',
                   'ack_stream_name': 'primary_ack'},
        setup_args={'output_stream_name': 'primary_data_stream',
                    'ack_stream_name': 'primary_ack'})
    flux_secondary_consumer_op = graph.add(
        FluxConsumerOperator,
        name='flux_secondary_consumer',
        init_args={'replica_num': 1,
                   'output_stream_name': 'secondary_data_stream',
                   'ack_stream_name': 'secondary_ack'},
        setup_args={'output_stream_name': 'secondary_data_stream',
                    'ack_stream_name': 'secondary_ack'})

    flux_primary_producer_op = graph.add(
        FluxProducerOperator,
        name='flux_primary_producer',
        init_args={'replica_num': 0,
                   'output_stream_name': 'primary_producer'},
        setup_args={'output_stream_name': 'primary_producer'})
    flux_secondary_producer_op = graph.add(
        FluxProducerOperator,
        name='flux_secondary_producer',
        init_args={'replica_num': 1,
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
                   'replica_num': 0},
        setup_args={'output_stream_name': 'primary_failure'})

    secondary_failure_op = graph.add(
        FailureOperator,
        name='secondary_failure',
        init_args={'output_stream_name': 'secondary_failure',
                   'replica_num': 1},
        setup_args={'output_stream_name': 'secondary_failure'})

    controller_op = graph.add(
        ControllerOperator,
        name='controller')

    # Connect source to ingress
    # graph.connect([source_op], [sink_op])
    graph.connect([source_op], [flux_ingress_op])
    
    # Connect the ingress operator to the consumer replicas, vice versa for ACK
    graph.connect([flux_ingress_op],
                  [flux_primary_consumer_op, flux_secondary_consumer_op])
    graph.connect([flux_primary_consumer_op, flux_secondary_consumer_op],
                  [flux_ingress_op])

    # Connect consumer => failure => producer
    graph.connect([flux_primary_consumer_op], [primary_failure_op])
    graph.connect([flux_secondary_consumer_op], [secondary_failure_op])
    graph.connect([primary_failure_op], [flux_primary_producer_op])
    graph.connect([secondary_failure_op], [flux_secondary_producer_op])

    # Connect producer operators to the egress operator, and egress only to secondary
    graph.connect([flux_primary_producer_op, flux_secondary_producer_op],
                  [flux_egress_op])
    graph.connect([flux_egress_op], [flux_secondary_producer_op])

    # Connect egress to sink
    graph.connect([flux_egress_op], [sink_op])

    # Connect the controller op to the operators that need to receive node
    # failure messages. Consumers don't need to receive
    graph.connect([controller_op],
                  [flux_ingress_op, flux_primary_producer_op,
                   flux_secondary_producer_op, flux_egress_op] +
                  [primary_failure_op, secondary_failure_op])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)

