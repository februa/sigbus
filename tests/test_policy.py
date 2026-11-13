from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event

import pytest

from sigbus import LatestPolicy, Bus, ImmediateWait, InProcessTransport, PollingWait, StreamSpec
from test_transports import domain, publish


def test_wait_for_publication(domain):
    transport, spec = domain
    waiting = Event()
    bus = Bus(transport, policy=LatestPolicy(wait=PollingWait(timeout=2)))
    pub, sub = bus.publisher(spec), bus.subscriber(spec)

    def read():
        waiting.set()
        with sub.acquire() as frame:
            return frame.timestamp, frame.payload.copy()

    with ThreadPoolExecutor() as pool:
        future = pool.submit(read)
        assert waiting.wait(1)
        publish(pub, 123, 7)
        timestamp, data = future.result(timeout=3)
    assert timestamp == 123
    assert (data == 7).all()
    pub.close()
    sub.close()


def test_wait_for_writable_slot(domain):
    transport, spec = domain
    bus = Bus(transport, policy=LatestPolicy(wait=PollingWait(timeout=2)))
    pub, sub1, sub2 = bus.publisher(spec), bus.subscriber(spec), bus.subscriber(spec)
    publish(pub, 1, 1)
    lease1 = sub1.acquire()
    lease1.__enter__()
    publish(pub, 2, 2)
    with sub2.acquire() as held:
        publish(pub, 3, 3)
        # All slots are pinned or the current latest; wait must release its locks.
        original = pub._endpoint.acquire
        attempted = Event()

        def observed(timestamp):
            attempted.set()
            return original(timestamp)

        pub._endpoint.acquire = observed
        with ThreadPoolExecutor() as pool:
            future = pool.submit(publish, pub, 4, 4)
            assert attempted.wait(1)
            lease1.__exit__(None, None, None)
            future.result(timeout=3)
        assert (held.payload == 2).all()
    for endpoint in (pub, sub1, sub2):
        endpoint.close()


def test_timeout_cancellation_and_recovery(domain):
    transport, spec = domain
    stop = Event()
    bus = Bus(transport, policy=LatestPolicy(wait=PollingWait(interval=0.001, timeout=0.01, stop_event=stop)))
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    with pytest.raises(TimeoutError):
        with sub.acquire():
            pass
    stop.set()
    with pytest.raises(InterruptedError):
        with sub.acquire():
            pass
    stop.clear()
    publish(pub, 1, 1)
    with sub.acquire() as frame:
        assert frame.timestamp == 1
    pub.close()
    sub.close()


def test_ap_exception_is_not_retried_and_write_is_aborted(domain):
    transport, spec = domain
    bus = Bus(transport, policy=LatestPolicy(wait=PollingWait(timeout=0)))
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    error = BlockingIOError("application error")
    with pytest.raises(BlockingIOError) as raised:
        with pub.acquire(1) as frame:
            frame.payload[:] = 1
            raise error
    assert raised.value is error
    with pytest.raises(TimeoutError):
        with sub.acquire():
            pass
    publish(pub, 2, 2)
    with pytest.raises(BlockingIOError) as raised:
        with sub.acquire():
            raise error
    assert raised.value is error
    pub.close()
    sub.close()


def test_cancellation_wakes_long_wait():
    attempted, stop = Event(), Event()

    @contextmanager
    def unavailable():
        attempted.set()
        raise BlockingIOError
        yield  # Context manager that fails during entry.

    def acquire():
        with PollingWait(interval=60, stop_event=stop).acquire(unavailable):
            pass

    with ThreadPoolExecutor() as pool:
        future = pool.submit(acquire)
        assert attempted.wait(1)
        stop.set()
        with pytest.raises(InterruptedError):
            future.result(timeout=2)


def test_deadline_uses_monotonic_and_limits_sleep(monkeypatch):
    import sigbus.policy as module
    clock = [100.0]
    attempts = []
    delays = []
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])

    def sleep(delay):
        delays.append(delay)
        clock[0] += delay

    monkeypatch.setattr(module, "sleep", sleep)

    @contextmanager
    def unavailable():
        attempts.append(clock[0])
        raise BlockingIOError
        yield

    lease = PollingWait(interval=2, timeout=3).acquire(unavailable)
    clock[0] = 200.0  # Deadline starts at __enter__.
    with pytest.raises(TimeoutError):
        with lease:
            pass
    assert attempts == [200, 202]
    assert delays == [2, 1]


def test_exit_errors_are_not_retried():
    calls = []

    @contextmanager
    def failure_on_exit():
        calls.append(1)
        yield object()
        raise BlockingIOError("flush failed")

    with pytest.raises(BlockingIOError, match="flush failed"):
        with PollingWait(timeout=0).acquire(failure_on_exit):
            pass
    assert calls == [1]


def test_immediate_and_closed_endpoint_behavior():
    transport = InProcessTransport()
    spec = StreamSpec("x", (1,), "f8")
    for i, policy in enumerate((None, LatestPolicy(wait=ImmediateWait()))):
        spec = StreamSpec(f"x-{i}", (1,), "f8")
        sub = Bus(transport, policy=policy).subscriber(spec)
        with pytest.raises(BlockingIOError):
            with sub.acquire():
                pass
        sub.close()
    sub = Bus(transport, policy=LatestPolicy(wait=PollingWait())).subscriber(spec)
    sub.close()
    with pytest.raises(RuntimeError):
        with sub.acquire():
            pass
    transport.close()


@pytest.mark.parametrize("kwargs", [dict(interval=0), dict(interval=-1),
    dict(interval=float("nan")), dict(interval=float("inf")), dict(timeout=-1),
    dict(timeout=float("nan")), dict(timeout=float("inf"))])
def test_invalid_policy(kwargs):
    with pytest.raises(ValueError):
        PollingWait(**kwargs)


@pytest.mark.parametrize('wait', [ImmediateWait(), PollingWait()])
def test_wait_is_not_a_bus_policy(wait):
    transport = InProcessTransport()
    with pytest.raises(TypeError, match='communication Policy'):
        Bus(transport, policy=wait)
    transport.close()


def test_old_policy_names_removed():
    import sigbus
    import sigbus.policy
    for module in (sigbus, sigbus.policy):
        assert not hasattr(module, 'ImmediatePolicy')
        assert not hasattr(module, 'PollingPolicy')
    with pytest.raises(TypeError):
        PollingWait(reuse='all_readers')
