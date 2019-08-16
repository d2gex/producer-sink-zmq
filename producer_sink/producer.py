from producer_sink import push_peer


class Producer(push_peer.PushPeer):
    '''Producer End Peer using a Push socket
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.socket.connect(self.url)
