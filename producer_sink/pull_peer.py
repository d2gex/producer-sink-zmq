import zmq

from producer_sink import errors, ipeer

POLLING_TIMEOUT = 10


class PullPeer(ipeer.IPeer):
    '''class extending PeerInterface and implementing all abstract methods to avoid boilerplate code in
    the subclasses. This classed is in turn extended by PULL-end peers

    This class is not usually instantiated
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.socket_type != zmq.PULL:
            raise errors.PushPullError(f"'{self.__class__.__name__}' end instantiated with the wrong socket "
                                       f"type: {self.socket_type}")
        self.timeout = kwargs.get('timeout', POLLING_TIMEOUT)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def run(self, data=None):
        '''Polls the socket in order to know if there is anything available to pick
        '''
        poll = dict(self.poller.poll(self.timeout))
        any_receivable = poll.get(self.socket, None)
        if any_receivable:
            return self.socket.recv_json()
        return False
