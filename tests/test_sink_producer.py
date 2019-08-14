import pytest
import zmq
import multiprocessing
import time

from pymulproc import factory, mpq_protocol
from pushpull_zmq import errors, ipeer, sink, producer
from unittest.mock import patch
from tests import utils as test_utils, stubs


def call_sink(cls, url, peer_name, comm, loops):
    '''Call a sink-like stub
    '''
    s = cls(identity=peer_name, url=url, s_type=zmq.PULL, linger=0)
    s.mock(comm, loops=loops)


def call_producer(cls, url, peer_name, q_factory, parent_pid):
    '''Call a producer-like stub
    '''
    s = cls(identity=peer_name, url=url, linger=0, parent_pid=parent_pid)
    s.mock(q_factory.child(timeout=0.1), parent_pid=parent_pid)


def test_producer_instantiation():
    '''A producer should always be a 'PUSH' socket type and never given a wildcard-type of url
            '''

    # Wrong socket type
    identity = 'WrongSocketType'
    try:
        p_end = producer.Producer(
            url=test_utils.TCP_CONNECT_URL_SOCKET,
            identity=identity,
            s_type=zmq.PULL)

    except errors.PushPullError:
        pass
    else:
        p_end.clean()
        raise AssertionError(f"The following expected exception was not triggered: {errors.PushPullError} "
                             f"on socket {identity}")

    # A Producer cannot be given an url with a wildcard as it is always a connect with a specific connection to url
    identity = 'UrlWithoutWildCard'
    url_with_wild_card = test_utils.TCP_CONNECT_URL_SOCKET.replace(test_utils.TCP_CLIENT_ADDRESS, '*')
    try:
        p_end = producer.Producer(
            url=url_with_wild_card,
            identity=identity)

    except errors.PushPullError:
        pass
    else:
        p_end.clean()
        raise AssertionError(f"The following expected exception was not triggered: {errors.PushPullError} "
                             f"on socket {identity}")


def test_sink_instantiation():
    '''A sink should always be a 'PULL' socket type
    '''

    identity = 'WrongSocketType'
    try:
        sink_end = sink.Sink(
            url=test_utils.TCP_BIND_URL_SOCKET,
            identity=identity,
            s_type=zmq.PUSH
        )
    except errors.PushPullError:
        pass
    else:
        sink_end.clean()  # Ensure the socket is close if the test DOEST NOT fail
        raise AssertionError(f"The following expected exception was not triggered: {errors.PushPullError} on "
                             f"socket {identity}")

    with patch('zmq.Context.socket') as mock_socket:
        mock_socket.return_value.bind.side_effect = zmq.error.ZMQError()
        with pytest.raises(errors.PushPullError):
            sink.Sink(url=test_utils.TCP_BIND_URL_SOCKET, identity=identity, s_type=zmq.PULL)


def test_ipc_url_max_length():

    with pytest.raises(errors.PushPullError):
        sink.Sink(url=f"ipc://{'a' * (ipeer.MAX_IPC_URL_LENGTH + 1)}", identity='url_too_long', s_type=zmq.PULL)


def test_producer_sink_single_connection():
    ''' A producer pushes a single dictionary to the sink successfully
    '''

    pipe_factory = factory.PipeCommunication()
    parent_comm = pipe_factory.parent()
    child_com = pipe_factory.child()
    sink_process = multiprocessing.Process(target=call_sink,
                                           args=(stubs.SinkOKStub,
                                                 test_utils.TCP_BIND_URL_SOCKET,
                                                 "SinkOK",
                                                 child_com,
                                                 100))

    push_producer = producer.Producer(url=test_utils.TCP_CONNECT_URL_SOCKET, identity='ProducerOK', linger=0)

    message = {
        'identity': push_producer.identity,
        'data': 'Info from producer 1'
    }
    server_message = None

    try:
        sink_process.start()

        # (1) check the message was pushed successfully
        ret = push_producer.run(message)
        assert ret is True

        # (2) Wait for the message sent to the SinkOK peer, which will be sent back to us by using the Multiprocessing
        # PIPE channel
        stop = False
        loops = 10
        while not stop:
            time.sleep(0.1)
            loops -= 1
            if loops <= 0:
                stop = True
            else:
                server_message = parent_comm.receive()
                stop = True if server_message else False

        # (3) Ensure the loop was finish because we did receive the message we pushed
        assert stop and loops > 0
        assert server_message[mpq_protocol.S_PID_OFFSET + 2] == message

    except KeyboardInterrupt:
        pass
    finally:
        sink_process.join()
        push_producer.clean()


def test_producer_sink_not_available():
    '''A producer tries to push a message to a sink that is no longer available. After a few retries it
    should give up and NEVER block.

    The High Water Mark(HWM) has been set to 1 message per peer queue. This is to facilitate the scenario in which a
    sink is no longer available as otherwise the PUSH socket would still carry on trying to resend it to the PULL
    socket following the default KEEP ALIVE policy - underlying OS - and we don't want to go that way. Instead
    our producer will try to resend the discarded message a certain number of attempts.
    '''
    pipe_factory = factory.PipeCommunication()
    parent_comm = pipe_factory.parent()
    child_com = pipe_factory.child()
    sink_process = multiprocessing.Process(target=call_sink,
                                           args=(stubs.SinkNotAvailableStub,
                                                 test_utils.TCP_BIND_URL_SOCKET,
                                                 'SinkNotAvailableStub',
                                                 child_com,
                                                 10))
    push_producer = producer.Producer(url=test_utils.TCP_CONNECT_URL_SOCKET,
                                      identity='ProducerRetry',
                                      linger=0,
                                      sndhwm=1,
                                      time_out=0.1
                                      )
    message = {
        'identity': push_producer.identity,
        'data': 'Info form producer 1'
    }
    try:
        sink_process.start()

        # (1) Tell Server via Multiprocessing PIPE communication to die before even sending sms via ZMQ
        parent_comm.send(mpq_protocol.REQ_DIE)
        sink_process.join()

        # (2) send first message and reach high water mark in underlying queues
        ret = push_producer.run(message)
        assert ret is True

        # (3) try to send a second message
        ret = push_producer.run(message)
        assert not ret

    except KeyboardInterrupt:
        pass
    finally:
        sink_process.join()
        push_producer.clean()


def test_multiple_producer_one_sink():
    '''Test that multiple simultaneous pushes can be handled by a sink. It also tests a practical case
    of Multiprocessing queue communication.

    It creates 5 child producers that will be sending data to the sink as a PUSH-ends. Then the same producers too
    send the same data but now via Multiprocessing queues. At the end we compare that the two data received are
    exactly the same to assert that the sink can handle multiple connections.
    '''

    queue_factory = factory.QueueCommunication()
    parent_comm = queue_factory.parent(timeout=0.1)
    parent_pid = multiprocessing.current_process().pid

    # Create
    cname = "ProducerMultiplePushesStub"
    producers = []
    for offset in range(5):
        producers.append(multiprocessing.Process(target=call_producer,
                                                 args=(stubs.ProducerMultiplePushesStub,
                                                       test_utils.TCP_CONNECT_URL_SOCKET,
                                                       cname + "_" + str(offset),
                                                       queue_factory, parent_pid)))

    sink_pull = sink.Sink(
        url=test_utils.TCP_BIND_URL_SOCKET,
        identity="SinkReceiveMultiplePushes",
        s_type=zmq.PULL
        )

    all_client_data = []
    try:
        for client in producers:
            client.start()

        # (1) Let's wait a bit for PUSH sockets to send their data so that they appear in our local PULL socket queues
        time.sleep(0.5)
        for _ in range(len(producers)):
            client_data = sink_pull.run()
            assert client_data is not False  # The polling will always return real data after the waiting period
            all_client_data.append(client_data)
        assert len(all_client_data) == len(producers)

        # (2) Wait for children processes to finish
        for client in producers:
            client.join()

        # (3) Now we know all data sent by the client via PUSH it has too been done via queues
        results = []
        for _ in range(len(producers)):
            results.append(parent_comm.receive(func=lambda messages: messages[mpq_protocol.R_PID_OFFSET] == parent_pid))

        # (4) The queue should now be empty and all tasks done
        assert parent_comm.conn.empty()
        parent_comm.conn.join()

        # (5) Check they are the same elements
        a = set(message[mpq_protocol.S_PID_OFFSET + 2]['my_id'] for message in results)
        b = set(message['my_id'] for message in all_client_data)
        assert a == b

    except KeyboardInterrupt:
        pass
    finally:
        for client in producers:
            client.join()
        parent_comm.conn.join()
        sink_pull.clean()
