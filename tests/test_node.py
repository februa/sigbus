from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import monotonic, sleep

import numpy as np
import pytest

from sigbus import (Bus, FIFOPolicy, Node, NodeSendError, StateSpec,
                    StreamSpec, SyncPolicy)
from test_transports import make_transport


def eventually(predicate):
    deadline = monotonic() + 3
    while not predicate():
        assert monotonic() < deadline
        sleep(0.001)


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def setup(request, tmp_path):
    transport = make_transport(request.param, tmp_path, slots=3)
    bus = Bus(transport, policy=FIFOPolicy(overflow='block'))
    endpoints, nodes, specs = [], [], []

    def stream(name, *, batch_size=1, state=False, slots=None, policy=None):
        spec = (StateSpec(name, (2,), 'i8', slots=slots or 2) if state else
                StreamSpec(name, (2,), 'i8', batch_size=batch_size, slots=slots, rate_hz=100))
        specs.append(spec)
        options = {'policy': policy} if policy is not None else {}
        if state:
            from sigbus import StatePolicy
            options = {'policy': StatePolicy()}
        pub, sub = bus.publisher(spec, **options), bus.subscriber(spec, **options)
        endpoints.extend([pub, sub])
        return pub, sub

    def node():
        result = Node()
        nodes.append(result)
        return result

    yield stream, node, transport
    for item in nodes:
        item.discard(timeout=3)
    for endpoint in endpoints:
        endpoint.close()
    for spec in specs:
        transport.remove(spec)
    transport.close()


def put(pub, value):
    with pub.acquire(value) as frame:
        frame.payload[...] = value


def read(sub, value):
    with sub.acquire() as frame:
        assert frame.timestamp == value
        assert (frame.payload == value).all()


def test_group_commit_and_zero_copy(setup):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    output, downstream = stream('output')
    second, second_reader = stream('second')
    node = new_node()
    assert isinstance(node, Node)
    node.add_subscriber(source)
    node.add_publisher(output)
    node.add_publisher(second)
    put(upstream, 7)
    with node.acquire(timeout=1) as data:
        assert not data[source].payload.flags.writeable
        for endpoint in (output, second):
            data[endpoint].payload[...] = data[source].payload
        address = data[output].payload.ctypes.data
        retained = data[output].payload
        assert not downstream.has_frame()
        assert not second_reader.has_frame()
    assert not retained.flags.writeable
    assert not source.has_frame()
    node.drain(timeout=3)
    with downstream.acquire() as frame:
        assert frame.payload.ctypes.data == address
        assert (frame.payload == 7).all()
    read(second_reader, 7)


def test_partial_reservation_cancel_does_not_consume(setup):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    output, downstream = stream('output', slots=1)
    put(upstream, 8)
    put(output, 1)
    node = new_node()
    node.add_subscriber(source)
    node.add_publisher(output)
    mapped = []
    storage = source._endpoint._storage
    original = storage.frame_payload
    storage.frame_payload = lambda *args: (mapped.append(args), original(*args))[1]
    with pytest.raises(TimeoutError):
        with node.acquire(timeout=0.02):
            pytest.fail('output is full')
    assert mapped == []  # No ndarray access before the whole group is ready.
    assert source.has_frame()
    read(downstream, 1)
    with node.acquire(timeout=1) as data:
        data[output].payload[...] = data[source].payload
    node.drain(timeout=3)
    read(downstream, 8)


def test_reservations_progress_for_other_endpoints(setup):
    stream, new_node, _ = setup
    first, first_reader = stream('first')
    second, second_reader = stream('second')
    node = new_node()
    node.add_subscriber(first_reader)
    node.add_subscriber(second_reader)
    put(second, 2)
    stop = Event()
    with ThreadPoolExecutor() as pool:
        def run():
            with node.acquire(stop_event=stop, timeout=2):
                pytest.fail('missing first input')
        future = pool.submit(run)
        eventually(lambda: not second_reader.has_frame())
        stop.set()
        with pytest.raises(InterruptedError):
            future.result(timeout=3)
    assert second_reader.has_frame()


def test_ap_fallback_and_uncaught_error(setup):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    output, downstream = stream('output')
    node = new_node()
    node.add_subscriber(source)
    node.add_publisher(output)
    put(upstream, 3)
    with node.acquire() as data:
        try:
            raise ValueError('processing')
        except ValueError:
            data[output].payload[...] = -1
    node.drain(timeout=3)
    with downstream.acquire() as frame:
        assert (frame.payload == -1).all()
    put(upstream, 4)
    with pytest.raises(ValueError):
        with node.acquire() as data:
            data[output].payload[...] = 999
            raise ValueError('uncaught')
    assert not source.has_frame()
    assert not downstream.has_frame()


def test_send_retry_pins_output_allows_next_slot_and_bounds_capacity(setup, monkeypatch):
    stream, new_node, _ = setup
    output, downstream = stream('output', slots=2)
    node = new_node()
    node.add_publisher(output)
    storage = output._endpoint._storage
    original = storage.flush_slot
    retrying, allow = Event(), Event()
    calls = []

    def fail_until_allowed(slot):
        calls.append(slot)
        retrying.set()
        if not allow.is_set():
            raise OSError('transient publication error')
        original(slot)

    monkeypatch.setattr(storage, 'flush_slot', fail_until_allowed)
    for value in (1, 2):
        with node.acquire(value, timeout=1) as data:
            data[output].payload[...] = value
    assert retrying.wait(2)
    assert not downstream.has_frame()
    with pytest.raises(TimeoutError):
        with node.acquire(3, timeout=0.02):
            pytest.fail('queued slots must remain protected')
    assert node.drain(timeout=0.01) is False
    allow.set()
    node.drain(timeout=3)
    read(downstream, 1)
    read(downstream, 2)
    assert len(calls) > 2


def test_batch_send_does_not_clear_next_partial_buffer(setup, monkeypatch):
    stream, new_node, _ = setup
    output, downstream = stream('batch', batch_size=2)
    node = new_node()
    node.add_publisher(output)
    storage = output._endpoint._storage
    original = storage.flush_slot
    allow = Event()

    def paused(slot):
        if not allow.is_set():
            raise OSError('retry')
        original(slot)

    monkeypatch.setattr(storage, 'flush_slot', paused)
    for value in (1, 2, 3):
        with node.acquire(value, timeout=1) as data:
            data[output].payload[...] = value
    assert output.has_pending()
    allow.set()
    eventually(downstream.has_frame)
    read(downstream, 1)
    read(downstream, 2)
    assert output.has_pending()  # Sending a full slot preserves the next tail.
    assert node.drain(timeout=3) is True
    read(downstream, 3)
    assert not output.has_pending()


def test_close_drains_tail_and_closes_owned_endpoints(setup):
    stream, new_node, _ = setup
    output, downstream = stream('batch', batch_size=4)
    node = new_node()
    delayed_lease = output.acquire(0)
    node.add_publisher(output)
    with pytest.raises(RuntimeError):
        with delayed_lease:
            pass
    with node.acquire(6) as data:
        data[output].payload[...] = 6
        with pytest.raises(RuntimeError):
            node.close()
        with pytest.raises(RuntimeError):
            output.close()
    node.close(timeout=3)
    read(downstream, 6)
    assert output.closed
    with pytest.raises(RuntimeError, match='closed'):
        put(output, 7)


def test_fatal_send_error_is_visible_and_discardable(setup, monkeypatch):
    stream, new_node, _ = setup
    output, downstream = stream('output')
    node = new_node()
    node.add_publisher(output)
    storage = output._endpoint._storage

    def denied(slot):
        raise PermissionError('denied')

    monkeypatch.setattr(storage, 'flush_slot', denied)
    with node.acquire(1) as data:
        data[output].payload[...] = 1
    with pytest.raises(NodeSendError) as caught:
        node.drain(timeout=3)
    assert isinstance(caught.value.__cause__, PermissionError)
    with pytest.raises(NodeSendError):
        with node.acquire(2):
            pass
    node.discard(timeout=3)
    assert not downstream.has_frame()


def test_stop_and_close_cancel_waits(setup):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    node = new_node()
    node.add_subscriber(source)
    with ThreadPoolExecutor() as pool:
        def wait():
            with node.acquire():
                pytest.fail('no input')
        future = pool.submit(wait)
        eventually(lambda: node._busy)
        node.close(timeout=3)
        with pytest.raises((InterruptedError, RuntimeError)):
            future.result(timeout=3)
    assert source.closed
    put(upstream, 9)


def test_state_and_sync_policies(setup):
    stream, new_node, _ = setup
    output, downstream = stream('state', state=True, slots=1)
    node = new_node()
    node.add_publisher(output)
    for value in (1, 2):
        with node.acquire(value, timeout=1) as data:
            data[output].payload[...] = value
        node.drain(timeout=3)
        read(downstream, value)
        read(downstream, value)
    sync_output, sync_reader = stream('sync', policy=SyncPolicy())
    sync_node = new_node()
    sync_node.add_publisher(sync_output)
    with sync_node.acquire(1) as data:
        data[sync_output].payload[...] = 1
    sync_node.drain(timeout=3)
    with pytest.raises(TimeoutError):
        with sync_node.acquire(2, timeout=0.01):
            pass
    read(sync_reader, 1)
    with sync_node.acquire(2, timeout=1) as data:
        data[sync_output].payload[...] = 2
    sync_node.drain(timeout=3)
    read(sync_reader, 2)


def test_validation(setup):
    stream, new_node, _ = setup
    output, source = stream('x')
    node, other = new_node(), new_node()
    with pytest.raises(TypeError):
        node.add_publisher(source)
    node.add_publisher(output)
    with pytest.raises(RuntimeError):
        other.add_publisher(output)
    with pytest.raises(ValueError):
        with node.acquire():
            pass
    with node.acquire(1) as data:
        data[output].payload[...] = 1
        with pytest.raises(RuntimeError):
            with node.acquire(2):
                pass
    with pytest.raises(RuntimeError):
        node.add_subscriber(source)


def test_partial_commit_failure_never_sends_incomplete_group(setup, monkeypatch):
    stream, new_node, _ = setup
    first, first_reader = stream('first')
    second, second_reader = stream('second')
    node = new_node()
    node.add_publisher(first)
    node.add_publisher(second)

    def failed(*args):
        raise OSError('commit failed')

    monkeypatch.setattr(second._endpoint._storage, 'commit_reserved', failed)
    with pytest.raises(OSError, match='commit failed'):
        with node.acquire(1) as data:
            data[first].payload[...] = 1
            data[second].payload[...] = 1
    assert not first_reader.has_frame()
    assert not second_reader.has_frame()
    with pytest.raises(NodeSendError):
        node.drain(timeout=1)
    node.discard(timeout=3)
    assert not first_reader.has_frame()
    assert not second_reader.has_frame()


def test_removed_stream_releases_node_ownership(setup, monkeypatch):
    stream, new_node, transport = setup
    output, downstream = stream('removed')
    node = new_node()
    node.add_publisher(output)
    storage = output._endpoint._storage
    attempt = Event()

    def failed(slot):
        attempt.set()
        raise OSError('retry')

    monkeypatch.setattr(storage, 'flush_slot', failed)
    with node.acquire(1) as data:
        data[output].payload[...] = 1
    assert attempt.wait(3)
    transport.remove('removed', force=True)
    from sigbus import StreamRemovedError
    with pytest.raises((NodeSendError, StreamRemovedError)):
        node.drain(timeout=3)
    node.discard(timeout=3)
    output.close()


def test_metadata_send_retry_does_not_duplicate_publication(setup, monkeypatch):
    stream, new_node, _ = setup
    output, downstream = stream('metadata')
    storage = output._endpoint._storage
    if not hasattr(storage, '_save'):
        pytest.skip('persistent metadata test')
    original = storage._save
    attempts = []

    def fail_before_replace(state):
        if state['generation'] == 1 and len(attempts) < 2:
            attempts.append(1)
            raise OSError('metadata unavailable')
        original(state)

    monkeypatch.setattr(storage, '_save', fail_before_replace)
    node = new_node()
    node.add_publisher(output)
    with node.acquire(1) as data:
        data[output].payload[...] = 1
    node.drain(timeout=3)
    with downstream.acquire() as frame:
        assert frame.generation == 1
        assert (frame.payload == 1).all()
    assert len(attempts) == 2
    assert not downstream.has_frame()


def _node_child(kind, root, conn):
    transport = make_transport(kind, root)
    spec = StreamSpec('process', (2,), 'i8', rate_hz=100)
    source = Bus(transport).subscriber(spec)
    node = Node()
    node.add_subscriber(source)
    conn.send('ready')
    with node.acquire(timeout=5) as frames:
        conn.send(frames[source].payload.tolist())
    node.close(timeout=3)
    source.close()
    transport.close()
    conn.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_node_between_processes(kind, tmp_path):
    import multiprocessing as mp
    transport = make_transport(kind, tmp_path)
    output = Bus(transport).publisher(StreamSpec('process', (2,), 'i8', rate_hz=100))
    node = Node()
    node.add_publisher(output)
    ctx = mp.get_context('spawn')
    parent, child = ctx.Pipe()
    process = ctx.Process(target=_node_child, args=(kind, str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(10)
        assert parent.recv() == 'ready'
        with node.acquire(42) as frames:
            frames[output].payload[...] = 42
        node.drain(timeout=3)
        assert parent.poll(10)
        assert parent.recv() == [42, 42]
        process.join(5)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)
        parent.close()
        node.discard(timeout=3)
        output.close()
        transport.remove('process', force=True)
        transport.close()


def test_failed_cleanup_can_be_retried_by_close(setup, monkeypatch):
    stream, new_node, _ = setup
    upstream, source = stream('cleanup')
    node = new_node()
    node.add_subscriber(source)
    put(upstream, 1)
    storage = source._endpoint._storage
    original = storage.release

    def fail(*args, **kwargs):
        raise OSError('cleanup unavailable')

    monkeypatch.setattr(storage, 'release', fail)
    with pytest.raises(ValueError) as caught:
        with node.acquire():
            raise ValueError('AP failure')
    assert any('cleanup unavailable' in note for note in caught.value.__notes__)
    monkeypatch.setattr(storage, 'release', original)
    node.discard(timeout=3)
    assert source.closed


def test_sender_start_failure_keeps_committed_slots_discardable(setup, monkeypatch):
    stream, new_node, _ = setup
    output, downstream = stream('startup')
    node = new_node()
    node.add_publisher(output)

    def fail(thread):
        raise RuntimeError('cannot start sender')

    monkeypatch.setattr('sigbus.node.Thread.start', fail)
    with node.acquire(1) as data:
        data[output].payload[...] = 1
    with pytest.raises(NodeSendError):
        node.drain(timeout=1)
    node.discard(timeout=3)
    assert not downstream.has_frame()


def test_manual_checkout_across_passes(setup):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    output, downstream = stream('output')
    node = new_node()
    node.add_subscriber(source)
    node.add_publisher(output)
    put(upstream, 4)
    node.reserve('subscribe')
    inputs = node.checkout(timeout=1)
    node.reserve('publish')
    outputs = node.checkout(timeout=1)
    for i in range(2):
        outputs[output].payload[i] = inputs[source].payload[i] * 2
        assert not downstream.has_frame()
    node.commit()
    assert not outputs[output].payload.flags.writeable
    assert not source.has_frame()  # Input remains checked out until complete.
    node.complete()
    assert node.drain(timeout=3)
    with downstream.acquire() as frame:
        assert frame.timestamp == 4
        assert (frame.payload == 8).all()


def test_manual_abort_preserves_input_and_node_is_reusable(setup):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    output, downstream = stream('output')
    node = new_node()
    node.add_subscriber(source)
    node.add_publisher(output)
    put(upstream, 1)
    node.reserve()
    frames = node.checkout(timeout=1)
    frames[output].payload[...] = 99
    with pytest.raises(RuntimeError):
        node.reserve('publish')
    with pytest.raises(RuntimeError):
        with node.acquire():
            pass
    node.abort()
    assert source.has_frame()
    assert not downstream.has_frame()
    assert not frames[output].payload.flags.writeable
    with node.acquire(timeout=1) as frames:
        frames[output].payload[...] = frames[source].payload
    assert node.drain(timeout=3)
    read(downstream, 1)


def test_checkout_timeout_can_resume_same_reservation(setup):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    output, downstream = stream('output', slots=1)
    put(upstream, 2)
    put(output, 0)
    node = new_node()
    node.add_subscriber(source)
    node.add_publisher(output)
    node.reserve()
    with pytest.raises(TimeoutError):
        node.checkout(timeout=0)
    assert not source.has_frame()  # Still pinned, not consumed.
    read(downstream, 0)
    frames = node.checkout(timeout=1)
    frames[output].payload[...] = frames[source].payload
    node.commit()
    node.complete()
    assert node.drain(timeout=3)
    read(downstream, 2)


def test_with_calls_manual_api_and_blocks_manual_mix(setup, monkeypatch):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    output, downstream = stream('output')
    node = new_node()
    node.add_subscriber(source)
    node.add_publisher(output)
    calls = []
    for name in ('reserve', 'checkout', 'commit', 'complete', 'abort'):
        original = getattr(node, name)
        def wrapped(*args, _name=name, _original=original, **kwargs):
            calls.append(_name)
            return _original(*args, **kwargs)
        monkeypatch.setattr(node, name, wrapped)
    put(upstream, 1)
    with node.acquire() as frames:
        frames[output].payload[...] = 1
    assert calls == ['reserve', 'checkout', 'commit', 'complete']
    calls.clear()
    put(upstream, 2)
    with pytest.raises(ValueError):
        with node.acquire() as frames:
            raise ValueError('AP failure')
    assert calls == ['reserve', 'checkout', 'complete', 'abort']
    assert not source.has_frame()
    put(upstream, 3)
    with node.acquire() as frames:
        with pytest.raises(RuntimeError):
            node.abort()
        frames[output].payload[...] = 3


def test_endpoint_manual_operations_and_resource_transfer(setup):
    stream, new_node, _ = setup
    output, source = stream('standalone')
    resource = output._resources
    output.reserve(timestamp=5)
    frame = output.checkout(timeout=1)
    frame.payload[...] = 5
    with pytest.raises(RuntimeError):
        with output.acquire(6):
            pass
    node = new_node()
    with pytest.raises(RuntimeError):
        node.add_publisher(output)
    output.commit()
    assert output.drain(timeout=3)
    source.reserve()
    incoming = source.checkout(timeout=1)
    assert (incoming.payload == 5).all()
    source.abort()
    assert source.has_frame()
    source.reserve()
    source.checkout(timeout=1)
    source.complete()
    assert not source.has_frame()
    node.add_publisher(output)
    assert output._resources is resource
    with pytest.raises(RuntimeError):
        output.reserve(timestamp=6)
    with node.acquire(6) as frames:
        frames[output].payload[...] = 6
    assert node.close(timeout=3)
    assert output.closed
    read(source, 6)


def test_drain_flushes_tail_after_grace_and_timeout_keeps_data(setup, monkeypatch):
    stream, new_node, _ = setup
    output, downstream = stream('tail', batch_size=4)
    node = new_node()
    node.add_publisher(output)
    allow = Event()
    storage = output._endpoint._storage
    original = storage.flush_slot
    def retry(slot):
        if not allow.is_set():
            raise OSError('retry')
        original(slot)
    monkeypatch.setattr(storage, 'flush_slot', retry)
    with node.acquire(1) as frames:
        frames[output].payload[...] = 1
    began = monotonic()
    assert node.drain(grace_period=.02, timeout=.04) is False
    assert monotonic() - began >= .02
    assert not output.has_pending()  # Tail was sealed, not discarded.
    assert not downstream.has_frame()
    allow.set()
    assert node.drain(timeout=3)
    read(downstream, 1)


def test_drain_can_be_cancelled_and_retried(setup, monkeypatch):
    stream, new_node, _ = setup
    output, downstream = stream('cancel')
    node = new_node()
    node.add_publisher(output)
    allow = Event()
    storage = output._endpoint._storage
    original = storage.flush_slot
    def retry(slot):
        if not allow.is_set():
            raise OSError('retry')
        original(slot)
    monkeypatch.setattr(storage, 'flush_slot', retry)
    with node.acquire(1) as frames:
        frames[output].payload[...] = 1
    cancelled = Event()
    with ThreadPoolExecutor() as pool:
        future = pool.submit(node.drain, stop_event=cancelled)
        eventually(lambda: node._draining)
        cancelled.set()
        assert future.result(timeout=3) is False
    allow.set()
    assert node.drain(timeout=3)
    read(downstream, 1)


def test_drain_grace_allows_batch_to_fill_normally(setup):
    stream, new_node, _ = setup
    output, downstream = stream('grace', batch_size=2)
    node = new_node()
    node.add_publisher(output)
    with node.acquire(1) as frames:
        frames[output].payload[...] = 1
    with ThreadPoolExecutor() as pool:
        future = pool.submit(node.drain, grace_period=2, timeout=3)
        eventually(lambda: node._draining)
        with node.acquire(2) as frames:
            frames[output].payload[...] = 2
        assert future.result(timeout=3)
    with downstream.acquire_batch() as batch:
        assert batch.timestamps == (1, 2)


def test_abort_does_not_retract_committed_output(setup):
    stream, new_node, _ = setup
    upstream, source = stream('input')
    output, downstream = stream('output')
    node = new_node()
    node.add_subscriber(source)
    node.add_publisher(output)
    put(upstream, 1)
    node.reserve()
    frames = node.checkout(timeout=1)
    frames[output].payload[...] = 1
    node.commit()
    node.abort()  # Only input remains: return unread without undoing publication.
    assert source.has_frame()
    assert node.drain(timeout=3)
    read(downstream, 1)


@pytest.mark.parametrize('grouped', [False, True])
def test_flush_publishes_manual_tail_without_closing(setup, grouped):
    stream, make_node, _ = setup
    pub, sub = stream('flush-tail', batch_size=4)
    owner = make_node() if grouped else pub
    if grouped:
        owner.add_publisher(pub)
    owner.reserve(timestamp=7)
    frames = owner.checkout()
    frame = frames[pub] if grouped else frames
    frame.payload[...] = 7
    owner.commit()
    assert not sub.has_frame()
    assert owner.flush(timeout=3) is True
    read(sub, 7)
    assert not pub.closed
    # Flushing leaves the owner available for the next processing pass.
    owner.reserve(timestamp=8)
    frames = owner.checkout()
    frame = frames[pub] if grouped else frames
    frame.payload[...] = 8
    owner.commit()
    assert owner.flush(timeout=3) is True
    read(sub, 8)
    assert owner.close(timeout=3) is True
