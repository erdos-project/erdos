Operators
=========

Operators process data in ERDOS applications.

Operators receive messages from streams passed to the `connect` static method.
Operators also create streams on which they send messages. These streams must
created and returned by the `connect` method.
ERDOS expects developers to specify which streams operators read from and write
to. For more details, see the
`data streams documentation <data_streams.html>`__.

All operators must implement `erdos.Operator` abstract class.

Operators set up state in the `__init__` method. Operators should also
add callbacks to streams in `__init__`.

Implement execution logic by overriding the `run` method. This method may
contain a control loop or call methods that run regularly.
*Callbacks are not invoked while run executes.*


API
---
.. autoclass:: erdos.Operator
    :members: __init__, connect, run, id, config,

.. autoclass:: erdos.OperatorConfig
    :members: name, flow_watermarks, log_file_name, csv_log_file_name, profile_file_name

Examples
--------

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
