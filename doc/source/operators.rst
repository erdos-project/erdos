Operators
=========

An ERDOS operator receives data on :py:class:`ReadStreams <erdos.ReadStream>`,
and sends processed data on :py:class:`WriteStreams <erdos.WriteStream>`.
We provide a standard library of operators for common dataflow patterns
under :py:mod:`erdos.operators`.
While the standard operators are general and versatile, some applications may
implement custom operators to better optimize performance and take
fine-grained control over exection.

All operators must inherit from the :py:class:`~erdos.Operator` base class and
implement :py:meth:`~erdos.Operator.__init__` and
:py:meth:`~erdos.Operator.connect` methods.

* :py:meth:`~erdos.Operator.__init__` takes all
  :py:class:`ReadStreams <erdos.ReadStream>` from which the operator receives
  data, all :py:class:`WriteStreams <erdos.WriteStream>` on which the operator
  sends data, and any other arguments passed when calling
  :py:meth:`~erdos.Operator.connect`.
  Within :py:meth:`~erdos.Operator.__init__`, the state should be initialized,
  and callbacks may be registered across
  :py:class:`ReadStreams <erdos.ReadStream>`.

* The :py:meth:`~erdos.Operator.connect` method takes
  :py:class:`ReadStreams <erdos.ReadStream>` and returns
  :py:class:`WriteStreams <erdos.WriteStream>`
  which are all later passed to :py:meth:`~erdos.Operator.__init__` by ERDOS.
  The :py:class:`ReadStreams <erdos.ReadStream>` and
  :py:class:`WriteStreams <erdos.WriteStream>`
  must appear in the same order as in :py:meth:`~erdos.Operator.__init__`.

While ERDOS manages the execution of callbacks, some operators require
more finegrained control. Operators can take manual control over the
thread of execution by implementing
:py:meth:`Operator.run() <erdos.Operator.run>`,
and pulling data from :py:class:`ReadStreams <erdos.ReadStream>`.
*Callbacks are not invoked while run executes.*


Operator API
------------

.. autoclass:: erdos.Operator
    :members: __init__, connect, run, id, config,

.. autoclass:: erdos.OperatorConfig
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
