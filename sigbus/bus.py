from .policy import FIFOPolicy, StatePolicy
from .stream import StateSpec
from .resources import EndpointResources
from contextlib import contextmanager


class Bus:
    def __init__(self, adapter, *, policy=None):
        if policy is not None and not (callable(getattr(policy, "open_stream", None))
                                       and callable(getattr(policy, "acquire", None))):
            raise TypeError("policy must be a communication Policy; pass waiting methods as "
                            "FIFOPolicy(wait=PollingWait(...)), for example")
        self._adapter = adapter
        self._policy = policy

    def _open(self, stream_spec, mode, policy):
        policy = self._policy if policy is None else policy
        if policy is None:
            policy = StatePolicy() if isinstance(stream_spec, StateSpec) else FIFOPolicy()
        if not (callable(getattr(policy, "open_stream", None))
                and callable(getattr(policy, "acquire", None))):
            raise TypeError("policy must implement open_stream and acquire")
        return policy.open_stream(self._adapter, stream_spec, mode), policy

    def publisher(self, stream_spec, *, policy=None):
        endpoint, policy = self._open(stream_spec, "write", policy)
        return Publisher(endpoint, policy=policy)

    def subscriber(self, stream_spec, *, policy=None):
        endpoint, policy = self._open(stream_spec, "read", policy)
        return Subscriber(endpoint, policy=policy)


class _GuardedLease:
    def __init__(self, endpoint, lease):
        self.endpoint, self.lease = endpoint, lease

    def __enter__(self):
        with self.endpoint._ownership_lock:
            self.endpoint._check_direct()
            frame = self.lease.__enter__()
            self.endpoint._direct_active = True
            return frame

    def __exit__(self, *args):
        try:
            return self.lease.__exit__(*args)
        finally:
            self.endpoint._direct_active = False


class _NodeEndpoint:
    _manual_session = None
    _direct_active = False
    _stop_event = None

    @property
    def spec(self):
        return self._endpoint.spec

    @property
    def stop_event(self):
        return self._stop_event

    @stop_event.setter
    def stop_event(self, event):
        self._stop_event = event
        if self._manual_session is not None:
            self._manual_session.stop_event = event

    @property
    def closed(self):
        return self._resources.closed

    def _check_node(self):
        self._resources.check_owner(self)

    def _check_direct(self):
        self._check_node()
        if self._direct_active:
            raise RuntimeError("endpoint already holds a frame")
        session = self._manual_session
        if session is not None:
            with session._condition:
                if session._busy or session._draining or session._queue:
                    raise RuntimeError("finish manual endpoint work before using acquire()")

    def _session(self):
        """Use the same lifecycle engine as Node, retaining endpoint ownership."""
        with self._ownership_lock:
            self._check_node()
            if self._direct_active:
                raise RuntimeError("manual operations are not allowed inside acquire()")
            if self._manual_session is None:
                from .node import _Lifecycle
                session = _Lifecycle(stop_event=self.stop_event)
                session._resource_owner = self
                collection = session._outputs if isinstance(self, Publisher) else session._inputs
                collection.append(self)
                self._manual_session = session
            return self._manual_session

    def _prepare_transfer(self):
        session = self._manual_session
        if session is None:
            return
        with session._condition:
            if session._busy or session._draining or session._queue or session._unreleased:
                raise RuntimeError("finish manual endpoint work before Node registration")
            self._endpoint.check_idle()
            session._send_stop.set()
            session._condition.notify_all()
        if session._worker is not None:
            session._worker.join()
        session._closed = True
        self._manual_session = None

    def reserve(self, *, timestamp=None, timestamps=None):
        with self._ownership_lock:
            self._session().reserve(timestamp=timestamp, timestamps=timestamps)

    def checkout(self, *, timeout=None, stop_event=None):
        return self._session().checkout(timeout=timeout, stop_event=stop_event)[self]

    def checkout_batch(self, *, timeout=None, stop_event=None):
        return self._session().checkout_batch(timeout=timeout, stop_event=stop_event)[self]

    def commit(self):
        return self._session().commit()

    def complete(self):
        return self._session().complete()

    def abort(self):
        return self._session().abort()

    def drain(self, *, grace_period=0, timeout=None, stop_event=None):
        return self._session().drain(grace_period=grace_period, timeout=timeout, stop_event=stop_event)

    def discard(self, *, timeout=None):
        return self._session().discard(timeout=timeout)

    def close(self, *, timeout=None):
        if self.closed:
            return True
        self._check_node()
        if self._manual_session is not None:
            return self._manual_session.close(timeout=timeout)
        self._resources.close(self)
        return True

class Publisher(_NodeEndpoint):
    def __init__(self, endpoint, *, policy=None):
        self._resources = EndpointResources(endpoint, self)
        self._ownership_lock = self._resources.lock
        self._endpoint = endpoint
        self._policy = FIFOPolicy() if policy is None else policy

    def acquire(self, timestamp):
        self._check_direct()
        if isinstance(self._policy, StatePolicy):
            lease = self._policy.acquire_write(self._endpoint, timestamp)
        else:
            lease = self._policy.acquire(lambda: self._endpoint.acquire(timestamp))
        return _GuardedLease(self, lease)

    def has_pending(self) -> bool:
        return self._endpoint.has_pending()

    @contextmanager
    def acquire_batch(self, timestamps):
        with self._session().acquire_batch(timestamps=timestamps) as data:
            yield data[self]

    def flush(self, *, timeout=None, stop_event=None):
        with self._ownership_lock:
            self._check_node()
            if self._manual_session is not None:
                return self._manual_session.flush(timeout=timeout, stop_event=stop_event)
            self._endpoint.flush()

    def ack(self):
        """Respond without completing or releasing the current lease."""
        self._endpoint.ack()

class Subscriber(_NodeEndpoint):
    def __init__(self, endpoint, *, policy=None):
        self._resources = EndpointResources(endpoint, self)
        self._ownership_lock = self._resources.lock
        self._endpoint = endpoint
        self._policy = FIFOPolicy() if policy is None else policy

    def has_frame(self) -> bool:
        """Snapshot of availability (unread stream data or current state); never reserves."""
        return self._endpoint.has_frame()

    def acquire(self):
        self._check_direct()
        return _GuardedLease(self, self._policy.acquire(self._endpoint.acquire))

    def acquire_batch(self):
        """Acquire the unread part of one published slot, without crossing slots."""
        self._check_direct()
        return _GuardedLease(self, self._policy.acquire(self._endpoint.acquire_batch))

    def ack(self):
        """Respond without completing or releasing the current lease."""
        self._endpoint.ack()
