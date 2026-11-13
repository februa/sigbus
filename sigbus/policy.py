"""Endpoint delivery and waiting rules, independent of payload representation."""
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
import math
from threading import Event
from time import monotonic, sleep
from typing import Protocol


class Policy(Protocol):
    """Bus consumes these methods; policy constructors have no common signature."""
    def open_stream(self, adapter, spec, mode): ...
    def acquire(self, factory): ...


def _validate_delivery(reuse, no_readers):
    if reuse not in ("available", "all_readers", "fifo", "sync", "state"):
        raise ValueError("reuse must be available, all_readers, fifo, sync or state")
    if no_readers not in ("proceed", "wait", "hold"):
        raise ValueError("no_readers must be proceed, wait or hold")


@dataclass(frozen=True, slots=True)
class ImmediateWait:
    """Try once; propagate BlockingIOError when a frame cannot be acquired."""

    def acquire(self, factory):
        return factory()


@dataclass(frozen=True, slots=True)
class PollingWait:
    """Retry acquisition only. Durations are seconds; None means no deadline."""

    interval: float = 0.001
    timeout: float | None = None
    stop_event: Event | None = None

    def __post_init__(self):
        if not math.isfinite(self.interval) or self.interval <= 0:
            raise ValueError("interval must be finite and positive")
        if self.timeout is not None and (not math.isfinite(self.timeout) or self.timeout < 0):
            raise ValueError("timeout must be finite and nonnegative, or None")

    @contextmanager
    def acquire(self, factory):
        # Start the deadline on context entry, not when acquire() is called.
        deadline = None if self.timeout is None else monotonic() + self.timeout
        first = True
        with ExitStack() as stack:
            while True:
                if self.stop_event is not None and self.stop_event.is_set():
                    raise InterruptedError("frame acquisition cancelled")
                if not first and deadline is not None and monotonic() >= deadline:
                    raise TimeoutError("frame acquisition timed out")
                first = False
                try:
                    # Each failed lease is single-use; obtain a new one on retry.
                    frame = stack.enter_context(factory())
                except BlockingIOError:
                    delay = self.interval
                    if deadline is not None:
                        remaining = deadline - monotonic()
                        if remaining <= 0:
                            raise TimeoutError("frame acquisition timed out") from None
                        delay = min(delay, remaining)
                    if self.stop_event is None:
                        sleep(delay)
                    elif self.stop_event.wait(delay):
                        raise InterruptedError("frame acquisition cancelled") from None
                else:
                    break
            # AP processing and lease exit are deliberately outside the retry handler.
            yield frame


class _DeliveryPolicy:
    """Common Bus contract; constructors remain owned by individual policies."""
    def open_stream(self, adapter, spec, mode):
        return adapter.open_stream(spec, mode, reuse=self.reuse, no_readers=self.no_readers,
                                   overflow=getattr(self, "overflow", "block"),
                                   group=getattr(self, "group", None), ack=self.ack)

    def acquire(self, factory):
        return self.wait.acquire(factory)


@dataclass(frozen=True, slots=True)
class AckSettings:
    """Endpoint liveness, independent of lease completion. Seconds, monotonic clock."""
    interval: float = 0.5
    timeout: float = 3.0
    on_timeout: str = "exclude"

    def __post_init__(self):
        if (not math.isfinite(self.interval) or not math.isfinite(self.timeout)
                or not 0 < self.interval < self.timeout):
            raise ValueError("ACK requires 0 < interval < timeout, both finite")
        if self.on_timeout not in ("exclude", "raise"):
            raise ValueError("on_timeout must be exclude or raise")


@dataclass(frozen=True, slots=True)
class FIFOPolicy(_DeliveryPolicy):
    """Read retained frames in order; default overflow drops oldest unleased data."""
    wait: object = None
    no_readers: str = "hold"
    overflow: str = "drop_oldest"
    ack: AckSettings | None = None
    reuse = "fifo"

    def __post_init__(self):
        if self.overflow not in ("drop_oldest", "block"):
            raise ValueError("overflow must be drop_oldest or block")
        _validate_delivery(self.reuse, self.no_readers)
        if self.wait is None:
            object.__setattr__(self, "wait", ImmediateWait())


@dataclass(frozen=True, slots=True)
class ReadCompletePolicy(_DeliveryPolicy):
    """Allow the next write only after all current readers have returned."""
    wait: object = None
    no_readers: str = "proceed"
    ack: AckSettings | None = None
    reuse = "all_readers"

    def __post_init__(self):
        _validate_delivery(self.reuse, self.no_readers)
        if self.wait is None:
            object.__setattr__(self, "wait", ImmediateWait())


@dataclass(frozen=True, slots=True)
class LatestPolicy(_DeliveryPolicy):
    """Deliver only the latest generation, preserving active zero-copy leases."""
    wait: object = None
    no_readers: str = "proceed"
    ack: AckSettings | None = None
    reuse = "available"

    def __post_init__(self):
        _validate_delivery(self.reuse, self.no_readers)
        if self.no_readers == "hold":
            raise ValueError("latest delivery supports proceed or wait")
        if self.wait is None:
            object.__setattr__(self, "wait", ImmediateWait())


@dataclass(frozen=True, slots=True)
class SyncPolicy(_DeliveryPolicy):
    """Wait for lease returns by subscribers in the same stream/group."""
    group: str = "default"
    wait: object = None
    no_readers: str = "proceed"
    ack: AckSettings | None = None
    reuse = "sync"

    def __post_init__(self):
        if not isinstance(self.group, str) or not self.group:
            raise ValueError("group must be a nonempty string")
        _validate_delivery(self.reuse, self.no_readers)
        if self.no_readers == "hold":
            raise ValueError("Sync supports proceed or wait; members join future publications")
        if self.wait is None:
            object.__setattr__(self, "wait", ImmediateWait())


@dataclass(frozen=True, slots=True)
class StatePolicy(_DeliveryPolicy):
    """Reference the current state repeatedly; only waiting and ACK are configurable."""
    wait: object = None
    ack: AckSettings | None = None
    reuse = "state"
    no_readers = "proceed"

    def __post_init__(self):
        if self.wait is None:
            object.__setattr__(self, "wait", ImmediateWait())

    @contextmanager
    def acquire_write(self, endpoint, timestamp):
        # Intent covers all retries, and is removed on cancellation or timeout.
        with endpoint.write_request():
            with self.acquire(lambda: endpoint.acquire(timestamp)) as frame:
                yield frame
