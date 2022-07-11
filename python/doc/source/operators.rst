Operators
=========

ERDOS operators process received data, and use streams to broadcast :py:class:`erdos.Message`
and :py:class:`erdos.WatermarkMessage` objects to downstream operators.
We provide a standard library of operators for common dataflow patterns.
While the standard operators are general and versatile, some applications may
implement custom operators to better optimize performance and take
fine-grained control over exection.

Operators are implemented as classes which implement a certain communication pattern. The built-in 
operators are subclassed based on the wanted communication pattern. For example, 
the `SendOp` from 
`python/examples/simple_pipeline.py <https://github.com/erdos-project/erdos/blob/master/python/examples/simple_pipeline.py>`_ 
implements a :py:class:`erdos.operator.Source` operator because it does not receive any data, 
and sends messages on a single output stream.

* The :py:class:`erdos.operator.Source` operator is used to write data on a single :py:class:`erdos.WriteStream`.
* The :py:class:`erdos.operator.Sink` operator is used to read data from a single :py:class:`erdos.ReadStream`.
* The :py:class:`erdos.operator.OneInOneOut` operator is used to read data from a single :py:class:`erdos.ReadStream`
  and write data on a single :py:class:`erdos.WriteStream`.
* The :py:class:`erdos.operator.TwoInOneOut` operator is used to read data from 2 :py:class:`erdos.ReadStream`
  s and write data on a single :py:class:`erdos.WriteStream`.
* The :py:class:`erdos.operator.OneInTwoOut` operator is used to read data from a single :py:class:`erdos.ReadStream`
  s and write data on 2 :py:class:`erdos.WriteStream` s.

Operators can support both push and pull-based models of execution by 
implementing methods defined for each operator. By implementing callbacks 
such as :py:meth:`erdos.operator.OneInOneOut.on_data()`, operators can process 
messages as they arrive. Moreover, operators can implement callbacks over watermarks 
(e.g. :py:meth:`erdos.operator.OneInOneOut.on_watermark()`) to ensure ordered 
processing over timestamps. ERDOS ensures lock-free, safe, and concurrent processing 
via a system-managed ordering of callbacks, which is implemented as a 
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
    :show-inheritance:
    :members: __new__, run, destroy

.. autoclass:: erdos.operator.Sink
    :show-inheritance:
    :members: __new__, run, on_data, on_watermark, destroy

.. autoclass:: erdos.operator.OneInOneOut
    :show-inheritance:
    :members: __new__, run, on_data, on_watermark, destroy

.. autoclass:: erdos.operator.TwoInOneOut
    :show-inheritance:
    :members: __new__, run, on_left_data, on_right_data, on_watermark, destroy

.. autoclass:: erdos.operator.OneInTwoOut
    :show-inheritance:
    :members: __new__, run, on_data, on_watermark, destroy

Operator Config
---------------

.. autoclass:: erdos.config.OperatorConfig
    :members: name, flow_watermarks, log_file_name, csv_log_file_name, profile_file_name

Context API
-----------

.. autoclass:: erdos.context.SinkContext
    :members:

.. autoclass:: erdos.context.OneInOneOutContext
    :members:

.. autoclass:: erdos.context.TwoInOneOutContext
    :members:

.. autoclass:: erdos.context.OneInTwoOutContext
    :members:

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
