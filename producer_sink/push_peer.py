import time
import zmq

from producer_sink import errors, ipeer

NUM_ATTEMPTS = 10


class PushPeer(ipeer.IPeer):
    '''class extending PeerInterface and implementing all abstract methods to avoid boilerplate code in
    the subclasses. This classed is in turn extended by PUSH-end peers

    This class is not usually instantiated
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.socket_type != zmq.PUSH:
            raise errors.PushPullError(f"'{self.__class__.__name__}' end instantiated with the wrong socket type: "
                                       f"{self.socket_type}")

        self.sndhwm = kwargs.get('sndhwm', 1000)  # 1000 is the default value according to zeromq
        self.socket.setsockopt(zmq.SNDHWM, self.sndhwm)
        self.mute_time_out = kwargs.get('time_out', 1)

    def run(self, data=None):
        '''It sends a message downstream following a round-robin approach according to ZeroMQ documentation

        If the other end is not available, it will keep storing outgoing messages on its queue until the queue is full.
        At that point the default behaviour is to block indefinitely however a system of retries have been implemented
        until certain amount attempts have been reached.
        '''
        stop = False
        loops = NUM_ATTEMPTS
        while not stop and loops:
            try:
                self.socket.send_json(data, flags=zmq.NOBLOCK)
            except zmq.error.Again:  # an exception is thrown if the message can't be sent because the queue is full
                loops -= 1
                time.sleep(self.mute_time_out)  # Give some time to the other end to recover
            else:
                stop = True
        return stop
