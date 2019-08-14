import zmq

from pushpull_zmq import errors, push_peer


class Producer(push_peer.PushPeer):
    '''Producer End Peer using a Push socket
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.socket.connect(self.url)
        except zmq.error.ZMQError as ex:
            raise errors.PushPullError(f"The url: {self.url} is not appropriate for a '{self.__class__.__name__}' end") \
                from ex
