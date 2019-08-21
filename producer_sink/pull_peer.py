import zmq

from producer_sink import ipeer

POLLING_TIMEOUT = 10


class PullPeer(ipeer.IPeer):
    '''class extending PeerInterface and implementing all abstract methods to avoid boilerplate code in
    the subclasses. This classed is in turn extended by PULL-end peers

    This class is not usually instantiated
    '''

    def __init__(self, *args, timeout=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout
        self.socket_type = zmq.PULL
        self.socket = self.context.socket(self.socket_type)
        self.socket.setsockopt(zmq.LINGER, self.linger)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def run(self):
        '''Polls the socket in order to know if there is anything available to pick
        '''
        poll = dict(self.poller.poll(self.timeout))
        any_receivable = poll.get(self.socket, None)
        if any_receivable:
            return self.socket.recv_json()
        return False
