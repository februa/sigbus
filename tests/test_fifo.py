from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
from threading import Event

import pytest

from sigbus import (Bus, FIFOPolicy, LatestPolicy, ReadCompletePolicy,
                    PollingWait, StreamSpec)
from test_transports import make_transport, publish


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def queue(request, tmp_path):
    transport = make_transport(request.param, tmp_path, 10)
    spec = StreamSpec('fifo', (8,), 'f8')
    yield transport, spec
    if request.param != 'inprocess':
        transport.remove(spec)
    transport.close()


def test_ten_publishes_then_ten_reads(queue):
    transport, spec = queue
    bus = Bus(transport, policy=FIFOPolicy(overflow="block"))
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    for i in range(10):
        publish(pub, i, i)
    with pytest.raises(BlockingIOError):
        publish(pub, 10, 10)
    for i in range(10):
        with sub.acquire() as frame:
            assert frame.timestamp == i
            assert (frame.payload == i).all()
    with pytest.raises(BlockingIOError):
        with sub.acquire():
            pass
    for i in range(10, 20):
        publish(pub, i, i)
    sub.close()
    pub.close()


def test_late_subscribers_all_return_exception_and_close(queue):
    transport, spec = queue
    pub = Bus(transport, policy=FIFOPolicy(overflow="block")).publisher(spec)
    for i in range(10):
        publish(pub, i, i)  # No readers: retained to capacity.
    first = Bus(transport, policy=FIFOPolicy(overflow="block")).subscriber(spec)
    second = Bus(transport, policy=FIFOPolicy(overflow="block")).subscriber(spec)
    with first.acquire() as frame:
        assert frame.timestamp == 0
        with pytest.raises(BlockingIOError):
            publish(pub, 10, 10)
    with pytest.raises(BlockingIOError):
        publish(pub, 10, 10)  # second still owes frame 0.
    late = Bus(transport, policy=FIFOPolicy(overflow="block")).subscriber(spec)
    with second.acquire():
        pass
    with pytest.raises(BlockingIOError):
        publish(pub, 10, 10)  # late is also included.
    with pytest.raises(KeyboardInterrupt):
        with late.acquire() as frame:
            assert frame.timestamp == 0
            raise KeyboardInterrupt
    publish(pub, 10, 10)
    late.close()
    second.close()
    for i in range(1, 11):
        with first.acquire() as frame:
            assert frame.timestamp == i
    first.close()
    pub.close()


def test_concurrent_reservations_commit_order_and_abort(queue):
    transport, spec = queue
    bus = Bus(transport)
    a, b, sub = bus.publisher(spec), bus.publisher(spec), bus.subscriber(spec)
    with a.acquire(100) as frame:
        frame.payload[:] = 100
        publish(b, 200, 200)
        with sub.acquire() as read:
            assert read.timestamp == 200
    with sub.acquire() as read:
        assert read.timestamp == 100
    with pytest.raises(ValueError):
        with a.acquire(300):
            raise ValueError
    with pytest.raises(BlockingIOError):
        with sub.acquire():
            pass
    a.close()
    b.close()
    sub.close()


@pytest.mark.parametrize('kind', ['inprocess', 'file', 'shared_memory'])
def test_single_slot_polling(kind, tmp_path):
    transport = make_transport(kind, tmp_path, 1)
    spec = StreamSpec('single', (8,), 'f8')
    bus = Bus(transport, policy=FIFOPolicy(wait=PollingWait(timeout=2), overflow="block"))
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    publish(pub, 0, 0)
    attempted = Event()
    original = pub._endpoint.acquire

    def observed(timestamp):
        attempted.set()
        return original(timestamp)

    pub._endpoint.acquire = observed
    with ThreadPoolExecutor() as pool:
        future = pool.submit(publish, pub, 1, 1)
        assert attempted.wait(1)
        with sub.acquire() as frame:
            assert frame.timestamp == 0
        future.result(timeout=3)
    with sub.acquire() as frame:
        assert frame.timestamp == 1
    sub.close()
    pub.close()
    if kind != 'inprocess':
        transport.remove(spec)
    transport.close()


@pytest.mark.parametrize('no_readers', ['proceed', 'wait'])
def test_no_reader_rules(no_readers, tmp_path):
    transport = make_transport('inprocess', tmp_path, 1)
    spec = StreamSpec('rules', (8,), 'f8')
    bus = Bus(transport, policy=FIFOPolicy(no_readers=no_readers))
    pub = bus.publisher(spec)
    if no_readers == 'wait':
        with pytest.raises(BlockingIOError):
            publish(pub, 1, 1)
    else:
        for i in range(10):
            publish(pub, i, i)
    sub = bus.subscriber(spec)
    with pytest.raises(BlockingIOError):
        with sub.acquire():
            pass
    publish(pub, 11, 11)
    sub.close()
    if no_readers == 'wait':
        with pytest.raises(BlockingIOError):
            publish(pub, 12, 12)
    else:
        publish(pub, 12, 12)
    pub.close()
    transport.close()


def fifo_child(kind, root, conn):
    transport = make_transport(kind, root, 10)
    spec = StreamSpec('fifo', (8,), 'f8')
    sub = Bus(transport).subscriber(spec)
    values = []
    for _ in range(10):
        with sub.acquire() as frame:
            values.append((frame.timestamp, float(frame.payload[0])))
    sub.close()
    transport.close()
    conn.send(values)
    conn.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_reopen_and_spawn_fifo(kind, tmp_path):
    transport = make_transport(kind, tmp_path, 10)
    spec = StreamSpec('fifo', (8,), 'f8')
    pub = Bus(transport).publisher(spec)
    for i in range(10):
        publish(pub, i, i)
    pub.close()
    transport.close()
    ctx = mp.get_context('spawn')
    parent, child = ctx.Pipe()
    process = ctx.Process(target=fifo_child, args=(kind, str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(15)
        assert parent.recv() == [(i, float(i)) for i in range(10)]
        process.join(10)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        parent.close()
    transport = make_transport(kind, tmp_path, 10)
    transport.remove(spec)
    transport.close()


@pytest.mark.parametrize('policy', [LatestPolicy(), ReadCompletePolicy()])
def test_named_policies(queue, policy):
    transport, spec = queue
    bus = Bus(transport, policy=policy)
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    publish(pub, 1, 1)
    if isinstance(policy, ReadCompletePolicy):
        with pytest.raises(BlockingIOError):
            publish(pub, 2, 2)
        expected = 1
    else:
        publish(pub, 2, 2)
        expected = 2
    with sub.acquire() as frame:
        assert frame.timestamp == expected
    pub.close()
    sub.close()


def test_custom_constructor_and_bus_contract(tmp_path):
    class CustomPolicy:
        def __init__(self, arbitrary_argument):
            self.value = arbitrary_argument
            self.delegate = FIFOPolicy()

        def open_stream(self, adapter, spec, mode):
            return self.delegate.open_stream(adapter, spec, mode)

        def acquire(self, factory):
            return self.delegate.acquire(factory)

    transport = make_transport('inprocess', tmp_path, 1)
    policy = CustomPolicy({'user': 'configuration'})
    bus = Bus(transport, policy=policy)
    spec = StreamSpec('custom', (8,), 'f8')
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    publish(pub, 1, 1)
    with sub.acquire() as frame:
        assert frame.timestamp == 1
    pub.close()
    sub.close()
    transport.close()
