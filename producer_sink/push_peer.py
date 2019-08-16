import time
import zmq

from producer_sink import ipeer


class PushPeer(ipeer.IPeer):
    '''class extending PeerInterface and implementing all abstract methods to avoid boilerplate code in
    the subclasses. This classed is in turn extended by PUSH-end peers

    This class is not usually instantiated
    '''

    def __init__(self, timeout=1, num_attempts=5, context=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout
        self.num_attempts = num_attempts
        self.sndhwm = kwargs.get('sndhwm', 1000)  # 1000 is the default value according to Zeromq
        self.socket_type = zmq.PUSH
        self.context = context or zmq.Context()
        self.socket = self.context.socket(self.socket_type)
        self.socket.setsockopt(zmq.LINGER, self.linger)
        self.socket.setsockopt(zmq.SNDHWM, self.sndhwm)

    def run(self, data):
        '''It sends a message downstream following a round-robin approach according to ZeroMQ documentation

        If the other end is not available, it will keep storing outgoing messages on its queue until the queue is full.
        At that point the default behaviour is to block indefinitely however a system of retries have been implemented
        until certain amount attempts have been reached.
        '''
        stop = False
        loops = self.num_attempts
        while not stop and loops:
            try:
                self.socket.send_json(data, flags=zmq.NOBLOCK)
            except zmq.error.Again:  # an exception is thrown if the message can't be sent because the queue is full
                loops -= 1
                time.sleep(self.timeout)  # Give some time to the other end to recover
            else:
                stop = True
        return stop
