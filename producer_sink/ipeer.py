import abc
import zmq

from producer_sink import errors

MAX_IPC_URL_LENGTH = 133


class IPeer(abc.ABC):
    '''Class Interface from which the different Push/Pull end peers will need to extend. It provides a common structure
    and clutter common code around its constructor
    '''

    def __init__(self, url, identity, context=None, s_type=zmq.PUSH, **kwargs):
        if 'ipc://' in url and len(url) > MAX_IPC_URL_LENGTH:
            raise errors.PushPullError(f"IPC URL '{url}' cannot have more than {MAX_IPC_URL_LENGTH} characters long")
        self.context = context or zmq.Context()
        self.socket = self.context.socket(s_type)
        self.socket.setsockopt(zmq.LINGER, kwargs.get('linger', -1))  # -1 is the default value according to zeromq
        self.url = url
        self.identity = identity
        self.socket_type = s_type

    @abc.abstractmethod
    def run(self, data=None):
        pass

    def clean(self):
        if not self.socket.closed:
            self.socket.close()
        if not self.context.closed:
            self.context.term()
