===================================================================================
``Producer-Sink-ZMQ``: A tiny library for producer-sink asynchronous communication
===================================================================================

A tiny library that abstract the producer-sink asynchronous communication pattern by using PUSH and PULL socket types
from ZeroMQ.

.. image:: https://travis-ci.com/d2gex/producer-sink-zmq.svg?branch=master
    :target: https://travis-ci.com/d2gex/producer-sink-zmq

.. image:: https://img.shields.io/badge/coverage-100%25-brightgreen.svg
    :target: #

Install and Run
===============
Producer-Sink-ZMQ is not available on PyPI yet, so you need to install it with pip providing a GitHub path as
follows::

    $ pip install git+https://github.com/d2gex/producer-sink-zmq.git@0.1.1#egg=producer-sink-zmq


.. code-block:: python

    ''' A producer that sends a sequence of number in descending order starting at 10 '''

    producer = producer.Producer(url=tcp://127.0.0.1:5556, identity='Producer Name', linger=0)
    loops = 10
    while loops:
        producer.run(loops)
        loops -= 1

where:

1.  **url**: Follows a format  `protocol://ip:port`. TCP, IPC and INPROC protocols are supported
2.  **linger**: Time i seconds for the socket to linger around until can close, once the close statement has been issued


.. code-block:: python

    ''' A sink that receives a sequence of number in descending order starting at 10 '''

    sink = producer.Sink(url=tcp://127.0.0.1:5556, identity='Sink Name', linger=0, s_type=zmq.PULL)
    loops = 10
    while loops:
        data = producer.run()
        print(data)
        loops -= 1
