Operators
=========

ERDOS operators process received data, and use streams to broadcast Messages 
to downstream operators.
We provide a standard library of operators for common dataflow patterns
under :py:mod:`erdos.operator`.
While the standard operators are general and versatile, some applications may
implement custom operators to better optimize performance and take
fine-grained control over exection.

Operators are structures which implement a certain communication pattern. For example, 
the `SendOp` from 
`python/examples/simple_pipeline.py <https://github.com/erdos-project/erdos/blob/master/python/examples/simple_pipeline.py>`_ 
implements a :py:class:`erdos.operator.Source` operator because it does not receive any data, 
and sends messages on a single output stream.

Operators can support both push and pull-based models of execution by 
implementing methods defined for each operator. By implementing callbacks 
such as :py:meth:`erdos.operator.OneInOneOut.on_data()`, operators can process 
messages as they arrive. Moreover, operators can implement callbacks over watermarks 
(e.g. :py:meth:`erdos.operator.OneInOneOut.on_watermark()`) to ensure ordered 
processing over timestamps. ERDOS ensures lock-free, safe, and concurrent processing 
by ordering callbacks in an ERDOS-managed execution lattice, which serves as a 
run queue for the system's multithreaded runtime.

While ERDOS manages the execution of callbacks, some operators require
more finegrained control. Operators can take manual control over the
thread of execution by implementing the `run()` 
(e.g. :py:meth:`erdos.operator.OneInOneOut.run()`) method.
*Callbacks are not invoked while run executes.*

Operator API
------------

.. autoclass:: erdos.operator.BaseOperator
    :members: id, config, add_trace_event, get_runtime

.. autoclass:: erdos.operator.Source
    :members: __new__, run, destroy

.. autoclass:: erdos.operator.Sink
    :members: __new__, run, on_data, on_watermark, destroy

.. autoclass:: erdos.operator.OneInOneOut
    :members: __new__, run, on_data, on_watermark, destroy

.. autoclass:: erdos.operator.TwoInOneOut
    :members: __new__, run, on_left_data, on_right_data, on_watermark, destroy

.. autoclass:: erdos.operator.OneInTwoOut
    :members: __new__, run, on_data, on_watermark, destroy

Operator Config
---------------

.. autoclass:: erdos.operator.OperatorConfig
    :members: name, flow_watermarks, log_file_name, csv_log_file_name, profile_file_name 

Examples
--------

Full example at `python/examples/simple_pipeline.py <https://github.com/erdos-project/erdos/blob/master/python/examples/simple_pipeline.py>`_.


Periodically Publishing Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: _literalinclude/python_examples/simple_pipeline.py
    :pyobject: SendOp


Processing Data via Callbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: _literalinclude/python_examples/simple_pipeline.py
    :pyobject: CallbackOp

Processing Data by Pulling Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: _literalinclude/python_examples/simple_pipeline.py
    :pyobject: PullOp
