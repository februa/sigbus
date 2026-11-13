import copy

import pytest

from sigbus import Bus, FIFOPolicy, LatestPolicy, ReadCompletePolicy, PollingWait
from test_transports import domain, publish


@pytest.mark.parametrize('policy', [FIFOPolicy(), LatestPolicy(), ReadCompletePolicy(),
                                    FIFOPolicy(wait=PollingWait(timeout=None))])
def test_snapshot_does_not_consume_or_wait(domain, policy):
    transport, spec = domain
    bus = Bus(transport, policy=policy)
    pub, sub, other = bus.publisher(spec), bus.subscriber(spec), bus.subscriber(spec)
    assert sub.has_frame() is False
    publish(pub, 1, 1)
    storage = sub._endpoint._storage
    with storage.transaction(read_only=True) as state:
        before = copy.deepcopy(state)
    for _ in range(3):
        assert sub.has_frame() is True
    with storage.transaction(read_only=True) as state:
        assert state == before
    with sub.acquire() as frame:
        assert frame.timestamp == 1
        assert sub.has_frame() is False  # This endpoint cannot acquire twice.
        assert other.has_frame() is True
    assert sub.has_frame() is False
    assert other.has_frame() is True
    for endpoint in (pub, sub, other):
        endpoint.close()
    with pytest.raises(RuntimeError, match='closed'):
        sub.has_frame()


def test_drain_retained_fifo(domain):
    transport, spec = domain
    bus = Bus(transport)
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    for i in range(10):
        publish(pub, i, i)
    received = []
    while sub.has_frame():
        with sub.acquire() as frame:
            received.append(frame.timestamp)
    assert received == [7, 8, 9]
    pub.close()
    sub.close()
