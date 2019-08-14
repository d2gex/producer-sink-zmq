import abc
import time
import multiprocessing

from pymulproc import mpq_protocol
from pushpull_zmq import sink, producer


class StubBase:
    @abc.abstractmethod
    def mock(self, process_comm, **kwargs):
        '''Method that needs to be implemented in the subclasses and that will be used to generate the test cases of
        this peer side.
        '''
        pass


class SinkOKStub(StubBase, sink.Sink):

    def mock(self, process_comm, **kwargs):
        '''It will fetch data sent from the client via a PULL socket and return it back to the client by using a
        Multiprocessing PIPE.
        '''

        try:
            stop = False
            # Polling for about 1s = 100 * 10ms(POLLING_TIMEOUT)
            loops = kwargs.get('loops', 100)
            while not stop and loops:
                loops -= 1
                client_data = self.run()
                if client_data:
                    process_comm.send(mpq_protocol.REQ_FINISHED, data=client_data)
                    stop = True
            assert stop and loops
        except KeyboardInterrupt:
            pass
            # ... close both the socket and context
        finally:
            self.clean()


class SinkNotAvailableStub(StubBase, sink.Sink):

    def mock(self, process_comm, **kwargs):
        '''It will wait up to 1 second for a signal sent by the client via the Multiprocessing PIPE to die ignoring its
        socket to emulate no availability
        '''

        stop = False
        loops = 10
        try:
            while not stop and loops:
                loops -= 1
                time.sleep(0.1)
                poison_pill = process_comm.receive()
                stop = True if poison_pill else False
        except KeyboardInterrupt:
            pass
        finally:
            self.clean()

        assert stop and loops


class ProducerMultiplePushesStub(StubBase, producer.Producer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pid = multiprocessing.current_process().pid

    def mock(self, process_comm, **kwargs):
        '''It will send a message to the Sink and then again via a Multiprocessing queue.

        This class will be instantiated and started several time by a parent process. This means that each child
        processes running this class will require some time to prepare their queues before sending data to the sink
        '''
        data = {
            'my_id': self.identity
        }
        try:
            time.sleep(0.1)  # Give this process some time to get its queues prepared
            self.run(data)
            process_comm.send(mpq_protocol.REQ_FINISHED, recipient_pid=kwargs.get('parent_pid', None), data=data)
        except KeyboardInterrupt:
            pass
        finally:
            self.clean()
