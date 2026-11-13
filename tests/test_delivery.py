from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from sigbus import ReadCompletePolicy, LatestPolicy, Bus, ImmediateWait, PollingWait, StreamSpec
from test_transports import make_transport, publish


@pytest.fixture(params=[(kind, slots) for kind in ('inprocess', 'file', 'shared_memory')
                        for slots in (1, 3)])
def delivery(request, tmp_path):
    kind, slots = request.param
    transport = make_transport(kind, tmp_path, slots)
    spec = StreamSpec('delivery', (8,), 'f8')
    yield transport, spec
    if kind != 'inprocess':
        transport.remove(spec)
    transport.close()


def test_all_readers_late_join_exception_and_close(delivery):
    transport, spec = delivery
    bus = Bus(transport, policy=ReadCompletePolicy())
    pub, first = bus.publisher(spec), bus.subscriber(spec)
    publish(pub, 1, 1)
    with pytest.raises(BlockingIOError):
        publish(pub, 2, 2)
    with pytest.raises(KeyboardInterrupt):
        with first.acquire():
            with pytest.raises(RuntimeError):
                first.close()
            raise KeyboardInterrupt
    late = bus.subscriber(spec)
    with pytest.raises(BlockingIOError):
        publish(pub, 2, 2)
    late.close()  # Unread subscriber leaves the waiting set.
    publish(pub, 2, 2)
    with first.acquire() as frame:
        assert frame.timestamp == 2
    # Joining during a write is included in the NEXT publication.
    with pub.acquire(3) as frame:
        frame.payload[:] = 3
        joined = bus.subscriber(spec)
        with pytest.raises(BlockingIOError):
            with joined.acquire():
                pass
    with first.acquire():
        pass
    with pytest.raises(BlockingIOError):
        publish(pub, 4, 4)
    with joined.acquire() as frame:
        assert frame.timestamp == 3
    first.close()
    joined.close()
    publish(pub, 4, 4)  # no_readers=proceed
    pub.close()


def test_no_readers_wait_and_abort(delivery):
    transport, spec = delivery
    bus = Bus(transport, policy=ReadCompletePolicy(wait=PollingWait(timeout=0), no_readers='wait'))
    pub = bus.publisher(spec)
    with pytest.raises(TimeoutError):
        publish(pub, 1, 1)
    sub = bus.subscriber(spec)
    publish(pub, 1, 1)
    with sub.acquire():
        pass
    with pytest.raises(ValueError):
        with pub.acquire(2) as frame:
            frame.payload[:] = 999
            raise ValueError('abort')
    with pytest.raises(TimeoutError):
        with sub.acquire():
            pass
    publish(pub, 3, 3)
    sub.close()  # Removes pending, but zero-reader policy still blocks.
    with pytest.raises(TimeoutError):
        publish(pub, 4, 4)
    late = bus.subscriber(spec)
    with late.acquire() as frame:
        assert frame.timestamp == 3
    publish(pub, 4, 4)
    late.close()
    pub.close()


def test_polling_unblocks_on_close(delivery):
    transport, spec = delivery
    bus = Bus(transport, policy=ReadCompletePolicy(wait=PollingWait(timeout=2)))
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    publish(pub, 1, 1)
    attempted = Event()
    original = pub._endpoint.acquire

    def observed(timestamp):
        attempted.set()
        return original(timestamp)

    pub._endpoint.acquire = observed
    with ThreadPoolExecutor() as pool:
        future = pool.submit(publish, pub, 2, 2)
        assert attempted.wait(1)
        sub.close()
        future.result(timeout=3)
    pub.close()


def test_policy_conflict(delivery):
    transport, spec = delivery
    bus = Bus(transport, policy=ReadCompletePolicy())
    pub = bus.publisher(spec)
    with pytest.raises(ValueError):
        Bus(transport).publisher(spec)
    reader = Bus(transport).subscriber(spec)
    reader.close()
    pub.close()


def test_no_reader_rule_independent(tmp_path):
    transport = make_transport('inprocess', tmp_path)
    spec = StreamSpec('independent', (8,), 'f8')
    bus = Bus(transport, policy=LatestPolicy(no_readers='wait'))
    pub = bus.publisher(spec)
    with pytest.raises(BlockingIOError):
        publish(pub, 1, 1)
    sub = bus.subscriber(spec)
    publish(pub, 1, 1)
    publish(pub, 2, 2)  # Does not wait for reads under reuse=available.
    with sub.acquire() as frame:
        assert frame.timestamp == 2
    sub.close()
    pub.close()
    transport.close()


def process_reader(kind, root, conn):
    transport = make_transport(kind, root, 1)
    spec = StreamSpec('process', (8,), 'f8')
    sub = Bus(transport, policy=ReadCompletePolicy()).subscriber(spec)
    conn.send('joined')
    conn.recv()
    try:
        with sub.acquire() as frame:
            conn.send((frame.timestamp, frame.payload.tolist()))
            conn.recv()
            raise KeyboardInterrupt
    except KeyboardInterrupt:
        pass
    finally:
        sub.close()
        transport.close()
        conn.send('closed')
        conn.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_process_reader_return_and_reopen(kind, tmp_path):
    import multiprocessing as mp

    transport = make_transport(kind, tmp_path, 1)
    spec = StreamSpec('process', (8,), 'f8')
    policy = ReadCompletePolicy()
    pub = Bus(transport, policy=policy).publisher(spec)
    publish(pub, 1, 1)
    ctx = mp.get_context('spawn')
    parent, child = ctx.Pipe()
    process = ctx.Process(target=process_reader, args=(kind, str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(10)
        assert parent.recv() == 'joined'
        with pytest.raises(BlockingIOError):
            publish(pub, 2, 2)
        parent.send('read')
        assert parent.poll(10)
        assert parent.recv() == (1, [1.0] * 8)
        with pytest.raises(BlockingIOError):
            publish(pub, 2, 2)
        parent.send('return')
        assert parent.poll(10)
        assert parent.recv() == 'closed'
        process.join(10)
        assert process.exitcode == 0
        publish(pub, 2, 2)
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        parent.close()
        pub.close()
    transport.close()
    transport = make_transport(kind, tmp_path, 1)
    sub = Bus(transport, policy=policy).subscriber(spec)
    pub = Bus(transport, policy=policy).publisher(spec)
    with pytest.raises(BlockingIOError):
        publish(pub, 3, 3)
    with sub.acquire() as frame:
        assert frame.timestamp == 2
    publish(pub, 3, 3)
    sub.close()
    pub.close()
    transport.remove(spec)
    transport.close()
