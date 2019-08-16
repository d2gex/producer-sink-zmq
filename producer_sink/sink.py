from producer_sink import pull_peer


class Sink(pull_peer.PullPeer):
    '''Sink End Peer using a Pull socket
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.socket.bind(self.url)
