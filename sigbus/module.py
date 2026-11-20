"""SigBus module lifecycle and ownership of application endpoints and Nodes."""
from threading import Event

from .node import Node


class Module:
    """SigBus processing module; application code keeps explicit acquisition.

    Specs and names remain application declarations. Endpoint resources are
    created automatically and transfer to Node on registration.
    """
    def __init__(self, bus, specs, *, interval=0.001):
        self.stop_event = Event()
        self.bus = bus
        self.specs = specs
        self.interval = interval
        self.nodes = []
        self._endpoints = []

    def bind_stop_event(self, event):
        self.stop_event = event
        for node in self.nodes:
            node.stop_event = event
        for endpoint in self._endpoints:
            endpoint.stop_event = event

    def initialize(self):
        """Create application connections before processing starts."""
        pass

    def run_once(self):
        """Perform one application step and return a Worker RunResult."""
        raise NotImplementedError

    def publisher(self, name, **kwargs):
        endpoint = self.bus.publisher(self.specs[name], **kwargs)
        endpoint.stop_event = self.stop_event
        self._endpoints.append(endpoint)
        return endpoint

    def subscriber(self, name, **kwargs):
        endpoint = self.bus.subscriber(self.specs[name], **kwargs)
        endpoint.stop_event = self.stop_event
        self._endpoints.append(endpoint)
        return endpoint

    def make_node(self, *, subscribers=(), publishers=()):
        node = Node(interval=self.interval, stop_event=self.stop_event)
        self.nodes.append(node)
        for endpoint in subscribers:
            node.add_subscriber(endpoint)
            if endpoint in self._endpoints:
                self._endpoints.remove(endpoint)
        for endpoint in publishers:
            node.add_publisher(endpoint)
            if endpoint in self._endpoints:
                self._endpoints.remove(endpoint)
        return node

    def drain(self):
        """Drain owned Nodes and standalone endpoints; False means cancellation."""
        for node in self.nodes:
            if not node.drain():
                return False
        for endpoint in self._endpoints:
            if not endpoint.closed and not endpoint.drain():
                return False
        return True

    def finalize(self):
        errors = []
        for node in reversed(self.nodes):
            try:
                node.abort()
                if self.stop_event.is_set():
                    node.discard()
                elif not node.close():
                    node.discard()
            except BaseException as exc:
                errors.append(exc)
                try:
                    node.discard()
                except BaseException as cleanup:
                    exc.add_note(f"Node discard failed: {cleanup!r}")
        # Unregistered endpoints still own their automatically created resources.
        for endpoint in reversed(self._endpoints):
            if not endpoint.closed:
                try:
                    endpoint.abort()
                    if self.stop_event.is_set():
                        endpoint.discard()
                    elif not endpoint.close():
                        endpoint.discard()
                except BaseException as exc:
                    errors.append(exc)
        if errors:
            for exc in errors[1:]:
                errors[0].add_note(f"Additional cleanup failure: {exc!r}")
            raise errors[0]
