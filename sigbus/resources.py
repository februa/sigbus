"""Automatically paired endpoint resources with one explicit owner."""
from threading import RLock


class EndpointResources:
    def __init__(self, endpoint, owner):
        self.endpoint = endpoint
        self.owner = owner
        self.closed = False
        self.lock = RLock()

    def check_owner(self, owner):
        if self.closed:
            raise RuntimeError('endpoint is closed')
        if self.owner is not owner:
            raise RuntimeError('endpoint resources belong to a Node')

    def transfer(self, previous, owner):
        with self.lock:
            self.check_owner(previous)
            self.endpoint.check_idle()
            self.owner = owner

    def close(self, owner):
        with self.lock:
            if self.closed:
                return
            self.check_owner(owner)
            self.endpoint.close()
            self.closed = True
            self.owner = None
