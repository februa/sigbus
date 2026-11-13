"""State publication: stable zero-copy leases, writer priority and spec capacity."""
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
from threading import Event
from time import monotonic, sleep

import numpy as np
import pytest

from sigbus import (Bus, StateSpec, StreamSpec, StatePolicy, FIFOPolicy,
                    LatestPolicy, SyncPolicy, PollingWait, NoFrameAvailable,
                    NoWritableSlot)
from test_transports import make_transport, publish


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def state_domain(request, tmp_path):
    transport = make_transport(request.param, tmp_path, slots=9)
    yield transport
    for name in list(transport._storages):
        transport.remove(name, force=True)
    transport.close()


def endpoints(transport, spec, stack, policy=None):
    bus = Bus(transport)
    pub = bus.publisher(spec, policy=policy)
    stack.callback(pub.close)
    sub = bus.subscriber(spec)
    stack.callback(sub.close)
    return pub, sub


def read(sub, value):
    with sub.acquire() as frame:
        np.testing.assert_array_equal(frame.payload, value)
        assert not frame.payload.flags.writeable
        return frame.generation


def test_repeat_current_and_generation(state_domain):
    spec = StateSpec('state', (8,), 'f8')
    with ExitStack() as stack:
        pub, sub = endpoints(state_domain, spec, stack)
        assert len(pub._endpoint._storage.arrays) == 2
        assert not sub.has_frame()
        with pytest.raises(NoFrameAvailable), sub.acquire():
            pass
        publish(pub, 100, 3)
        for _ in range(3):
            assert sub.has_frame()
            assert read(sub, 3) == 1
        with sub.acquire() as frame:
            assert np.shares_memory(frame.payload, pub._endpoint._storage.arrays[0])
        publish(pub, 100, 4)  # Timestamp is not a generation identifier.
        assert read(sub, 4) == 2
        late = Bus(state_domain).subscriber(spec)
        stack.callback(late.close)
        assert read(late, 4) == 2
        with pytest.raises(ValueError, match='acquire_batch'):
            sub.acquire_batch()


@pytest.mark.parametrize('slots', [1, 2, 3])
def test_stable_leases_and_capacity(state_domain, slots):
    spec = StateSpec('state', (8,), 'f8', slots=slots)
    with ExitStack() as stack:
        pub, sub = endpoints(state_domain, spec, stack)
        publish(pub, 1, 1)
        with sub.acquire() as old:
            if slots == 1:
                with pytest.raises(NoWritableSlot):
                    publish(pub, 2, 2)
            else:
                publish(pub, 2, 2)
                if slots == 2:
                    with pytest.raises(NoWritableSlot):
                        publish(pub, 3, 3)
                else:
                    for i in range(3, 10):
                        publish(pub, i, i)
            np.testing.assert_array_equal(old.payload, 1)
        publish(pub, 20, 20)
        assert read(sub, 20) >= 2


@pytest.mark.parametrize('slots', [1, 2])
def test_abort_and_read_during_write(state_domain, slots):
    with ExitStack() as stack:
        pub, sub = endpoints(state_domain, StateSpec('state', (8,), 'f8', slots=slots), stack)
        publish(pub, 1, 1)
        with pytest.raises(RuntimeError, match='abort'):
            with pub.acquire(2) as frame:
                frame.payload[:] = 99
                if slots == 1:
                    assert not sub.has_frame()
                    with pytest.raises(NoFrameAvailable), sub.acquire():
                        pass
                else:
                    assert read(sub, 1) == 1
                raise RuntimeError('abort')
        if slots == 1:
            assert not sub.has_frame()
        else:
            assert read(sub, 1) == 1
        publish(pub, 3, 3)
        assert read(sub, 3) == 2


def test_single_slot_writer_priority_and_timeout_cleanup(state_domain):
    spec = StateSpec('state', (8,), 'f8', slots=1)
    with ExitStack() as stack:
        pub, sub = endpoints(state_domain, spec, stack,
                              StatePolicy(wait=PollingWait(timeout=2)))
        observer = Bus(state_domain).subscriber(spec)
        stack.callback(observer.close)
        publish(pub, 1, 1)
        with ThreadPoolExecutor(1) as pool:
            with sub.acquire() as frame:
                future = pool.submit(publish, pub, 2, 2)
                deadline = monotonic() + 1
                while observer.has_frame() and monotonic() < deadline:
                    sleep(.001)
                assert not observer.has_frame()
                assert not future.done()
                np.testing.assert_array_equal(frame.payload, 1)
            future.result(timeout=3)
        assert read(observer, 2) == 2
        timed = Bus(state_domain).publisher(spec, policy=StatePolicy(wait=PollingWait(timeout=.01)))
        stack.callback(timed.close)
        with sub.acquire():
            with pytest.raises(TimeoutError):
                publish(timed, 3, 3)
            assert read(observer, 2) == 2
        stop = Event()
        stop.set()
        cancelled = Bus(state_domain).publisher(spec, policy=StatePolicy(wait=PollingWait(stop_event=stop)))
        stack.callback(cancelled.close)
        with pytest.raises(InterruptedError):
            publish(cancelled, 3, 3)
        assert read(observer, 2) == 2


def test_spec_capacity_and_kind_validation(state_domain):
    with ExitStack() as stack:
        pub, sub = endpoints(state_domain, StateSpec('state', (8,), 'f8', slots=1), stack)
        assert len(pub._endpoint._storage.arrays) == 1
        bus = Bus(state_domain)
        for wrong in (StateSpec('state', (8,), 'f8', slots=2),
                      StreamSpec('state', (8,), 'f8', slots=1)):
            with pytest.raises(ValueError):
                bus.subscriber(wrong)
        for policy in (FIFOPolicy(), LatestPolicy(), SyncPolicy()):
            with pytest.raises(ValueError, match='StateSpec'):
                bus.subscriber(StateSpec('other', (8,), 'f8'), policy=policy)
        with pytest.raises(ValueError):
            bus.publisher(StreamSpec('other', (8,), 'f8'), policy=StatePolicy())
        signal = StreamSpec('signal', (8,), 'f8', slots=5)
        stream_pub, stream_sub = endpoints(state_domain, signal, stack)
        assert len(stream_pub._endpoint._storage.arrays) == 5
        for i in range(5):
            publish(stream_pub, i, i)
        for i in range(5):
            read(stream_sub, i)


@pytest.mark.parametrize('slots', [0, -1, 1.5])
@pytest.mark.parametrize('spec_class', [StateSpec, StreamSpec])
def test_bad_slot_count(slots, spec_class):
    with pytest.raises((ValueError, TypeError)):
        spec_class('bad', (1,), 'f8', slots=slots)


def _child_read_state(kind, root, ready, release, result):
    transport = make_transport(kind, root, slots=30)
    spec = StateSpec('state', (8,), 'f8')
    sub = Bus(transport).subscriber(spec)
    try:
        with sub.acquire() as frame:
            ready.set()
            assert release.wait(10)
            np.testing.assert_array_equal(frame.payload, 1)
        result.put(read(sub, 2))
        result.put(read(sub, 2))
    finally:
        sub.close()
        transport.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_cross_process_current_and_pin(tmp_path, kind):
    transport = make_transport(kind, tmp_path)
    spec = StateSpec('state', (8,), 'f8')
    pub = Bus(transport).publisher(spec)
    ctx = mp.get_context('spawn')
    ready, release, result = ctx.Event(), ctx.Event(), ctx.Queue()
    child = ctx.Process(target=_child_read_state, args=(kind, tmp_path, ready, release, result))
    try:
        publish(pub, 1, 1)
        child.start()
        assert ready.wait(10)
        publish(pub, 2, 2)
        with pytest.raises(NoWritableSlot):
            publish(pub, 3, 3)
        release.set()
        assert result.get(timeout=10) == 2
        assert result.get(timeout=10) == 2
        child.join(10)
        assert child.exitcode == 0
    finally:
        release.set()
        if child.pid is not None:
            child.join(2)
            if child.is_alive():
                child.terminate()
                child.join()
        pub.close()
        transport.remove(spec, force=True)
        transport.close()
        result.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_reopen_current_and_reject_old_format(tmp_path, kind):
    transport = make_transport(kind, tmp_path)
    spec = StateSpec('state', (8,), 'f8', slots=1)
    pub = Bus(transport).publisher(spec)
    publish(pub, 1, 7)
    pub.close()
    transport.close()
    other = make_transport(kind, tmp_path, slots=20)
    sub = Bus(other).subscriber(spec)
    assert read(sub, 7) == 1
    assert read(sub, 7) == 1
    sub.close()
    storage = other._storages[spec.name]
    with storage.transaction() as state:
        state['version'] = 6
    other.close()
    again = make_transport(kind, tmp_path)
    try:
        with pytest.raises(ValueError, match='version'):
            Bus(again).subscriber(spec)
    finally:
        again.remove(spec, force=True)
        again.close()


@pytest.mark.parametrize('slots', [1, 2])
def test_flush_failure_releases_writer_and_preserves_contract(state_domain, slots, monkeypatch):
    with ExitStack() as stack:
        pub, sub = endpoints(state_domain, StateSpec('state', (8,), 'f8', slots=slots), stack)
        publish(pub, 1, 1)
        storage = pub._endpoint._storage
        original = storage.flush_slot
        def fail_flush(slot):
            raise OSError('flush failed')
        monkeypatch.setattr(storage, 'flush_slot', fail_flush)
        with pytest.raises(OSError, match='flush failed'):
            publish(pub, 2, 2)
        if slots == 1:
            assert not sub.has_frame()
        else:
            assert read(sub, 1) == 1
        monkeypatch.setattr(storage, 'flush_slot', original)
        publish(pub, 3, 3)
        assert read(sub, 3) == 2


def test_all_old_generations_pinned_and_exception_return(state_domain):
    spec = StateSpec('state', (8,), 'f8', slots=3)
    with ExitStack() as stack:
        pub, a = endpoints(state_domain, spec, stack)
        b = Bus(state_domain).subscriber(spec)
        stack.callback(b.close)
        publish(pub, 1, 1)
        with a.acquire() as first:
            publish(pub, 2, 2)
            with pytest.raises(RuntimeError, match='reader failed'):
                with b.acquire() as second:
                    publish(pub, 3, 3)
                    with pytest.raises(NoWritableSlot):
                        publish(pub, 4, 4)
                    np.testing.assert_array_equal(first.payload, 1)
                    np.testing.assert_array_equal(second.payload, 2)
                    raise RuntimeError('reader failed')
            publish(pub, 4, 4)
            assert read(b, 4) == 4
        assert read(a, 4) == 4


def test_state_ack_exclusion_never_revokes_active_reader(state_domain):
    from sigbus import AckSettings, ParticipantExcludedError
    from test_endpoint_policies import expire
    with ExitStack() as stack:
        pub, ordinary = endpoints(state_domain, StateSpec('state', (8,), 'f8'), stack)
        watched = Bus(state_domain).subscriber(
            StateSpec('state', (8,), 'f8'), policy=StatePolicy(ack=AckSettings()))
        stack.callback(watched.close)
        publish(pub, 1, 1)
        with watched.acquire() as held:
            expire(watched)
            publish(pub, 2, 2)
            with pytest.raises(NoWritableSlot):
                publish(pub, 3, 3)
            np.testing.assert_array_equal(held.payload, 1)
        with pytest.raises(ParticipantExcludedError), watched.acquire():
            pass
        publish(pub, 3, 3)
        assert read(ordinary, 3) == 3
