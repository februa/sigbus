from .policy import FIFOPolicy, StatePolicy
from .stream import StateSpec


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

class Publisher:
    def __init__(self, endpoint, *, policy=None):
        self._endpoint = endpoint
        self._policy = FIFOPolicy() if policy is None else policy

    def acquire(self, timestamp):
        if isinstance(self._policy, StatePolicy):
            return self._policy.acquire_write(self._endpoint, timestamp)
        return self._policy.acquire(lambda: self._endpoint.acquire(timestamp))

    def has_pending(self) -> bool:
        return self._endpoint.has_pending()

    def flush(self) -> None:
        self._endpoint.flush()

    def ack(self):
        """Respond without completing or releasing the current lease."""
        self._endpoint.ack()

    def close(self):
        self._endpoint.close()

class Subscriber:
    def __init__(self, endpoint, *, policy=None):
        self._endpoint = endpoint
        self._policy = FIFOPolicy() if policy is None else policy

    def has_frame(self) -> bool:
        """Snapshot of availability (unread stream data or current state); never reserves."""
        return self._endpoint.has_frame()

    def acquire(self):
        return self._policy.acquire(self._endpoint.acquire)

    def acquire_batch(self):
        """Acquire the unread part of one published slot, without crossing slots."""
        return self._policy.acquire(self._endpoint.acquire_batch)

    def ack(self):
        """Respond without completing or releasing the current lease."""
        self._endpoint.ack()

    def close(self):
        self._endpoint.close()
