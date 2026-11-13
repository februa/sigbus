import pytest
from sigbus import Bus, FIFOPolicy, StreamSpec, NoFrameAvailable, NoWritableSlot
from test_transports import make_transport, publish


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def queue(request, tmp_path):
    transport = make_transport(request.param, tmp_path, 5)
    spec = StreamSpec('overflow', (8,), 'f8')
    yield transport, spec
    if request.param != 'inprocess':
        transport.remove(spec)
    transport.close()


def test_default_keeps_last_five_for_all_readers(queue):
    transport, spec = queue
    bus = Bus(transport)
    pub = bus.publisher(spec)
    readers = [bus.subscriber(spec), bus.subscriber(spec)]
    for i in range(10):
        publish(pub, i, i)
    readers.append(bus.subscriber(spec))
    for sub in readers:
        for i in range(5, 10):
            with sub.acquire() as frame:
                assert frame.timestamp == i
                assert (frame.payload == i).all()
        with pytest.raises(NoFrameAvailable, match='no unread frame'):
            with sub.acquire():
                pass
        sub.close()
    pub.close()


def test_pinned_oldest_is_skipped(queue):
    transport, spec = queue
    pub, sub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    publish(pub, 0, 0)
    with sub.acquire() as held:
        for i in range(1, 20):
            publish(pub, i, i)
        assert (held.payload == 0).all()
    for i in range(16, 20):
        with sub.acquire() as frame:
            assert frame.timestamp == i
    pub.close()
    sub.close()


def test_all_slots_leased_cannot_be_overwritten(queue):
    from contextlib import ExitStack
    transport, spec = queue
    bus = Bus(transport)
    pub = bus.publisher(spec)
    readers = []
    with ExitStack() as stack:
        for i in range(5):
            sub = bus.subscriber(spec)
            readers.append(sub)
            publish(pub, i, i)
            # Consume older retained frames before pinning the current one.
            for _ in range(i):
                with sub.acquire():
                    pass
            frame = stack.enter_context(sub.acquire())
            assert frame.timestamp == i
        with pytest.raises(NoWritableSlot, match='all slots are leased'):
            publish(pub, 5, 5)
    for sub in readers:
        sub.close()
    pub.close()


def test_drop_occurs_on_reservation_even_if_write_aborts(queue):
    transport, spec = queue
    pub = Bus(transport).publisher(spec)
    for i in range(5):
        publish(pub, i, i)
    with pytest.raises(ValueError):
        with pub.acquire(5) as frame:
            frame.payload[:] = 999
            raise ValueError('abort')
    sub = Bus(transport).subscriber(spec)
    for i in range(1, 5):
        with sub.acquire() as frame:
            assert frame.timestamp == i
            assert (frame.payload == i).all()
    with pytest.raises(NoFrameAvailable):
        with sub.acquire():
            pass
    pub.close()
    sub.close()


def test_overflow_policy_conflict(queue):
    transport, spec = queue
    pub = Bus(transport).publisher(spec)
    with pytest.raises(ValueError, match='policy mismatch'):
        Bus(transport, policy=FIFOPolicy(overflow='block')).publisher(spec)
    pub.close()


def test_invalid_overflow():
    with pytest.raises(ValueError):
        FIFOPolicy(overflow='invalid')
