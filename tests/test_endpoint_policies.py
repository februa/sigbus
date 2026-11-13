"""Mixed delivery, synchronous membership, and independent ACK control."""
import multiprocessing as mp
from time import monotonic, sleep

import numpy as np
import pytest

from sigbus import (AckSettings, AckTimeoutError, Bus, FIFOPolicy, LatestPolicy,
                    SyncPolicy, ParticipantExcludedError, StreamSpec, NoWritableSlot,
                    PollingWait, StreamRemovedError)
from test_transports import make_transport, publish


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def mixed(request, tmp_path):
    transport = make_transport(request.param, tmp_path, 3)
    spec = StreamSpec('mixed', (8,), 'f8')
    yield transport, spec
    transport.remove(spec, force=True)
    transport.close()


def read(sub, expected):
    with sub.acquire() as frame:
        assert frame.timestamp == expected
        np.testing.assert_array_equal(frame.payload, expected)


def stop_ack(endpoint):
    raw = endpoint._endpoint
    raw._stop.set()
    if raw._worker:
        raw._worker.join(2)
        assert not raw._worker.is_alive()


def expire(endpoint):
    stop_ack(endpoint)
    raw = endpoint._endpoint
    with raw._storage.transaction() as state:
        state['endpoints'][raw._endpoint_id]['last_ack'] = monotonic() - 100


def test_fifo_latest_independent_and_latest_retained(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    pub = bus.publisher(spec, policy=FIFOPolicy(overflow='block'))
    fifo = bus.subscriber(spec)
    latest = bus.subscriber(spec, policy=LatestPolicy())
    for i in range(3):
        publish(pub, i, i)
    read(latest, 2)
    for i in range(3):
        read(fifo, i)
    late = bus.subscriber(spec, policy=LatestPolicy())
    read(late, 2)  # FIFO completion did not retire the latest value.
    for i in range(3, 10):
        publish(pub, i, i)
        read(fifo, i)
    read(latest, 9)  # Not reading the display did not stall FIFO.
    for e in (late, latest, fifo, pub):
        e.close()


def test_latest_writer_fifo_reader_backpressure(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    pub = bus.publisher(spec, policy=LatestPolicy())
    fifo = bus.subscriber(spec)
    latest = bus.subscriber(spec, policy=LatestPolicy())
    for i in range(3):
        publish(pub, i, i)
    with pytest.raises(NoWritableSlot):
        publish(pub, 3, 3)
    read(fifo, 0)
    publish(pub, 3, 3)
    for i in range(1, 4):
        read(fifo, i)
    read(latest, 3)
    for e in (latest, fifo, pub):
        e.close()


def test_sync_group_snapshot_and_exception_completion(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    sync = SyncPolicy(group='analysis')
    pub = bus.publisher(spec, policy=sync)
    sub = bus.subscriber(spec, policy=sync)
    with pytest.raises(ValueError, match='same group'):
        bus.subscriber(spec, policy=SyncPolicy(group='other'))
    display = bus.subscriber(spec, policy=LatestPolicy())
    publish(pub, 1, 1)
    late = bus.subscriber(spec, policy=sync)
    assert not late.has_frame()
    with pytest.raises(BlockingIOError):
        publish(pub, 2, 2)
    with pytest.raises(ValueError):
        with sub.acquire():
            raise ValueError('calculation failed but lease returned')
    publish(pub, 2, 2)  # Late join didn't delay the previous generation.
    read(sub, 2)
    with pytest.raises(BlockingIOError):
        publish(pub, 3, 3)
    late.close()  # An unread target may leave normally.
    publish(pub, 3, 3)
    read(display, 3)
    for e in (display, sub, pub):
        e.close()


def test_sync_wait_needs_group_member(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    pub = bus.publisher(spec, policy=SyncPolicy(group='analysis', no_readers='wait'))
    display = bus.subscriber(spec, policy=LatestPolicy())
    with pytest.raises(BlockingIOError):
        publish(pub, 1, 1)
    sub = bus.subscriber(spec, policy=SyncPolicy(group='analysis'))
    publish(pub, 1, 1)
    for e in (sub, display, pub):
        e.close()


@pytest.mark.parametrize('policy', [FIFOPolicy, LatestPolicy, SyncPolicy])
def test_background_ack_during_lease_does_not_complete(mixed, policy):
    transport, spec = mixed
    bus = Bus(transport)
    pub = bus.publisher(spec, policy=policy())
    sub = bus.subscriber(spec, policy=policy(ack=AckSettings(interval=.01, timeout=1)))
    publish(pub, 1, 1)
    raw = sub._endpoint
    with sub.acquire() as frame:
        with raw._storage.transaction(read_only=True) as state:
            initial = state['endpoints'][raw._endpoint_id]['last_ack']
        deadline = monotonic() + 2
        while True:
            with raw._storage.transaction(read_only=True) as state:
                endpoint = state['endpoints'][raw._endpoint_id]
                if endpoint['last_ack'] > initial:
                    assert endpoint['active']
                    assert endpoint['status'] == 'joined'
                    break
            assert monotonic() < deadline
            sleep(.005)
        sub.ack()
        if policy is SyncPolicy:
            with pytest.raises(BlockingIOError):
                publish(pub, 2, 2)
        assert (frame.payload == 1).all()
    pub.close()
    sub.close()
    assert not raw._worker.is_alive()


@pytest.mark.parametrize('policy', [FIFOPolicy, LatestPolicy, SyncPolicy])
def test_exclusion_preserves_active_lease_and_requires_rejoin(mixed, policy):
    transport, spec = mixed
    bus = Bus(transport)
    pub = bus.publisher(spec, policy=policy())
    sub = bus.subscriber(spec, policy=policy(ack=AckSettings()))
    publish(pub, 1, 1)
    with sub.acquire() as frame:
        expire(sub)
        for i in range(2, 8):
            publish(pub, i, i)
        assert (frame.payload == 1).all()
        with pytest.raises(ParticipantExcludedError):
            sub.ack()
        with pytest.raises(ParticipantExcludedError):
            sub.has_frame()
    sub.close()  # Returning and closing remain possible after exclusion.
    rejoined = bus.subscriber(spec, policy=policy())
    publish(pub, 8, 8)
    if policy is SyncPolicy:
        read(rejoined, 8)
    rejoined.close()
    pub.close()


def test_exclusion_never_steals_single_slot(tmp_path):
    t = make_transport('inprocess', tmp_path, 1)
    spec = StreamSpec('single', (8,), 'f8')
    bus = Bus(t)
    pub = bus.publisher(spec, policy=SyncPolicy())
    sub = bus.subscriber(spec, policy=SyncPolicy(ack=AckSettings()))
    publish(pub, 1, 1)
    with sub.acquire() as frame:
        expire(sub)
        with pytest.raises(NoWritableSlot):
            publish(pub, 2, 2)
        assert (frame.payload == 1).all()
    publish(pub, 2, 2)
    sub.close()
    pub.close()
    t.close()


def test_timeout_raise_preserves_obligations(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    pub = bus.publisher(spec, policy=SyncPolicy(wait=PollingWait(timeout=1)))
    sub = bus.subscriber(spec, policy=SyncPolicy(ack=AckSettings(on_timeout='raise')))
    publish(pub, 1, 1)
    expire(sub)
    with pytest.raises(AckTimeoutError):
        publish(pub, 2, 2)
    with pytest.raises(AckTimeoutError):
        sub.has_frame()
    sub.close()
    publish(pub, 2, 2)
    pub.close()


def test_expired_writer_partial_is_reclaimed_but_active_is_pinned(mixed):
    transport, _ = mixed
    spec = StreamSpec('partial', (8,), 'f8', batch_size=2)
    bus = Bus(transport)
    a = bus.publisher(spec, policy=FIFOPolicy(ack=AckSettings()))
    b = bus.publisher(spec)
    sub = bus.subscriber(spec)
    publish(a, 1, 1)
    expire(a)
    with pytest.raises(ParticipantExcludedError):
        a.flush()
    publish(b, 2, 2)
    publish(b, 3, 3)
    with sub.acquire_batch() as batch:
        assert batch.timestamps == (2, 3)
    a.close()
    b.close()
    sub.close()
    transport.remove(spec)


def test_sync_batch_waits_until_last_return(mixed):
    transport, _ = mixed
    spec = StreamSpec('batch-sync', (8,), 'f8', batch_size=2)
    bus = Bus(transport)
    pub = bus.publisher(spec, policy=SyncPolicy())
    sub = bus.subscriber(spec, policy=SyncPolicy())
    display = bus.subscriber(spec, policy=LatestPolicy())
    publish(pub, 1, 1)
    assert not sub.has_frame()
    publish(pub, 2, 2)
    read(sub, 1)
    with pytest.raises(BlockingIOError):
        publish(pub, 3, 3)
    with sub.acquire_batch() as batch:
        assert batch.timestamps == (2,)
    publish(pub, 3, 3)
    pub.flush()
    read(sub, 3)
    read(display, 3)
    for e in (display, sub, pub):
        e.close()
    transport.remove(spec)


def process_member(kind, root, conn):
    t = make_transport(kind, root)
    spec = StreamSpec('process-sync', (8,), 'f8')
    sub = Bus(t).subscriber(spec, policy=SyncPolicy(ack=AckSettings(interval=.02, timeout=1)))
    conn.send('joined')
    conn.recv()
    with sub.acquire() as frame:
        conn.send(frame.timestamp)
        conn.recv()  # ACK worker continues while computation waits.
    sub.close()
    t.close()
    conn.send('closed')
    conn.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_cross_process_sync_ack_and_return(kind, tmp_path):
    t = make_transport(kind, tmp_path)
    spec = StreamSpec('process-sync', (8,), 'f8')
    pub = Bus(t).publisher(spec, policy=SyncPolicy())
    ctx = mp.get_context('spawn')
    parent, child = ctx.Pipe()
    process = ctx.Process(target=process_member, args=(kind, str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(10) and parent.recv() == 'joined'
        publish(pub, 1, 1)
        parent.send('read')
        assert parent.poll(10) and parent.recv() == 1
        raw = pub._endpoint
        with raw._storage.transaction(read_only=True) as state:
            key = next(k for k, v in state['endpoints'].items() if v['mode'] == 'read')
            initial = state['endpoints'][key]['last_ack']
        deadline = monotonic() + 3
        while True:
            with raw._storage.transaction(read_only=True) as state:
                if state['endpoints'][key]['last_ack'] > initial:
                    break
            assert monotonic() < deadline
            sleep(.01)
        with pytest.raises(BlockingIOError):
            publish(pub, 2, 2)
        parent.send('return')
        assert parent.poll(10) and parent.recv() == 'closed'
        process.join(10)
        assert process.exitcode == 0
        publish(pub, 2, 2)
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        parent.close()
        pub.close()
        t.remove(spec, force=True)
        t.close()


@pytest.mark.parametrize('kwargs', [dict(interval=0), dict(timeout=0), dict(interval=2, timeout=1),
                                  dict(timeout=float('inf')), dict(on_timeout='ignore')])
def test_invalid_ack(kwargs):
    with pytest.raises(ValueError):
        AckSettings(**kwargs)


def test_expired_active_writer_is_not_published_or_stolen(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    a = bus.publisher(spec, policy=FIFOPolicy(ack=AckSettings()))
    b = bus.publisher(spec)
    sub = bus.subscriber(spec)
    with pytest.raises(ParticipantExcludedError):
        with a.acquire(99) as frame:
            frame.payload[:] = 99
            expire(a)
            for i in range(6):
                publish(b, i, i)
            assert (frame.payload == 99).all()
    values = []
    while sub.has_frame():
        with sub.acquire() as frame:
            values.append(frame.timestamp)
    assert values == [4, 5]
    a.close()
    b.close()
    sub.close()


def test_expiration_does_not_clear_unrelated_hold_data(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    a = bus.publisher(spec, policy=FIFOPolicy(ack=AckSettings()))
    publish(a, 1, 1)
    expire(a)
    b = bus.publisher(spec)
    sub = bus.subscriber(spec)
    read(sub, 1)
    for e in (a, b, sub):
        e.close()


def test_close_during_lease_keeps_ack_worker(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    pub = bus.publisher(spec)
    sub = bus.subscriber(spec, policy=FIFOPolicy(ack=AckSettings()))
    publish(pub, 1, 1)
    with sub.acquire():
        with pytest.raises(RuntimeError):
            sub.close()
        assert sub._endpoint._worker.is_alive()
        sub.ack()
    sub.close()
    pub.close()


def test_ack_worker_error_is_surfaced_and_shutdown(mixed):
    transport, spec = mixed
    sub = Bus(transport).subscriber(spec, policy=LatestPolicy(ack=AckSettings(interval=.01, timeout=1)))
    transport.remove(spec, force=True)
    deadline = monotonic() + 2
    while sub._endpoint._worker.is_alive():
        assert monotonic() < deadline
        sleep(.005)
    with pytest.raises(StreamRemovedError):
        sub.has_frame()
    sub.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_dead_process_excluded_without_revoking_lease(kind, tmp_path):
    t = make_transport(kind, tmp_path)
    spec = StreamSpec('process-sync', (8,), 'f8')
    pub = Bus(t).publisher(spec, policy=SyncPolicy())
    ctx = mp.get_context('spawn')
    parent, child = ctx.Pipe()
    process = ctx.Process(target=process_member, args=(kind, str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(10) and parent.recv() == 'joined'
        publish(pub, 1, 1)
        parent.send('read')
        assert parent.poll(10) and parent.recv() == 1
        process.terminate()
        process.join(10)
        assert not process.is_alive()
        deadline = monotonic() + 5
        while True:
            try:
                publish(pub, 2, 2)
                break
            except BlockingIOError:
                assert monotonic() < deadline
                sleep(.02)
        raw = pub._endpoint
        with raw._storage.transaction(read_only=True) as state:
            key = next(k for k, e in state['endpoints'].items() if e['mode'] == 'read')
            assert state['endpoints'][key]['status'] == 'excluded'
            slot = next(i for i, s in enumerate(state['slots']) if key in s['readers'])
            assert state['slots'][slot]['barrier'] == []
            assert (raw._storage.arrays[slot] == 1).all()
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        parent.close()
        pub.close()
        t.remove(spec, force=True)
        t.close()


def test_sync_rejects_hold():
    with pytest.raises(ValueError, match='future publications'):
        SyncPolicy(no_readers='hold')


def test_ack_is_opt_in_and_endpoint_policy_overrides_legacy_bus(mixed):
    transport, spec = mixed
    bus = Bus(transport, policy=LatestPolicy())
    pub = bus.publisher(spec, policy=FIFOPolicy())
    sub = bus.subscriber(spec, policy=FIFOPolicy())
    assert pub._endpoint._worker is None
    assert sub._endpoint._worker is None
    publish(pub, 1, 1)
    publish(pub, 2, 2)
    read(sub, 1)
    read(sub, 2)
    pub.close()
    sub.close()


def test_reader_first_validates_sync_writer_and_failed_attach_is_clean(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    sub = bus.subscriber(spec, policy=SyncPolicy(group='g'))
    with pytest.raises(ValueError, match='same group'):
        bus.publisher(spec)
    with pytest.raises(ValueError, match='same group'):
        bus.publisher(spec, policy=SyncPolicy(group='wrong'))
    pub = bus.publisher(spec, policy=SyncPolicy(group='g'))
    publish(pub, 1, 1)
    read(sub, 1)
    pub.close()
    sub.close()


def test_excluded_sync_writer_does_not_block_other_free_slots(mixed):
    transport, spec = mixed
    bus = Bus(transport)
    a = bus.publisher(spec, policy=SyncPolicy(ack=AckSettings()))
    b = bus.publisher(spec, policy=SyncPolicy())
    sub = bus.subscriber(spec, policy=SyncPolicy())
    with pytest.raises(ParticipantExcludedError):
        with a.acquire(99) as frame:
            frame.payload[:] = 99
            expire(a)
            publish(b, 1, 1)
            read(sub, 1)
            assert (frame.payload == 99).all()
    for e in (a, b, sub):
        e.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_version_five_requires_explicit_migration(kind, tmp_path):
    import json
    t = make_transport(kind, tmp_path)
    spec = StreamSpec('old-format', (8,), 'f8')
    pub = Bus(t).publisher(spec)
    pub.close()
    path = t._stream_directory(spec.name) / 'metadata.json'
    state = json.loads(path.read_text())
    state['version'] = 5
    path.write_text(json.dumps(state))
    t.close()
    t = make_transport(kind, tmp_path)
    with pytest.raises(ValueError, match='unsupported storage version'):
        Bus(t).publisher(spec)
    t.remove(spec)
    t.close()


def test_expired_writer_return_is_not_aborted_twice(mixed, monkeypatch):
    transport, spec = mixed
    pub = Bus(transport).publisher(spec, policy=FIFOPolicy(ack=AckSettings()))
    raw = pub._endpoint._storage
    original = raw.release
    calls = []
    def record(*args):
        calls.append(args)
        return original(*args)
    monkeypatch.setattr(raw, 'release', record)
    with pytest.raises(ParticipantExcludedError):
        with pub.acquire(1) as frame:
            frame.payload[:] = 1
            expire(pub)
    assert len(calls) == 1
    pub.close()
