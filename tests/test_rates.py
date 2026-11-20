import pytest
import numpy as np

from sigbus import Node, Bus, StreamSpec, StateSpec, Batch, Frame, FIFOPolicy
from test_transports import make_transport


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def domain(request, tmp_path):
    transport = make_transport(request.param, tmp_path)
    bus = Bus(transport)
    endpoints, nodes, specs = [], [], []
    def pair(name, rate=100, batch=4, *, state=False):
        spec = StateSpec(name, (2,), 'i8') if state else StreamSpec(name, (2,), 'i8', rate_hz=rate, batch_size=batch)
        specs.append(spec)
        pub, sub = bus.publisher(spec), bus.subscriber(spec)
        endpoints.extend((pub, sub))
        return pub, sub
    def node():
        n = Node()
        nodes.append(n)
        return n
    yield bus, pair, node
    for n in nodes:
        n.abort()
        n.discard()
    for e in endpoints:
        if not e.closed:
            e.abort()
            e.discard()
    for spec in specs:
        transport.remove(spec)
    transport.close()


def test_rate_required_and_validated():
    with pytest.raises(TypeError):
        StreamSpec('x', (1,), 'f4')
    for invalid in (0, -1, float('inf'), float('nan'), True):
        with pytest.raises(ValueError):
            StreamSpec('x', (1,), 'f4', rate_hz=invalid)
    assert not hasattr(StateSpec('s', (1,), 'f4'), 'rate_hz')


def test_rate_mismatch_rejected_on_reopen(domain):
    bus, pair, _ = domain
    pair('x')
    with pytest.raises(ValueError, match='specification'):
        bus.subscriber(StreamSpec('x', (2,), 'i8', rate_hz=50, batch_size=4))


def test_registration_rejects_no_compatible_mode_without_taking_ownership(domain):
    _, pair, node = domain
    _, a = pair('a', rate=100, batch=4)
    b_pub, b = pair('b', rate=50, batch=3)
    n = node()
    n.add_subscriber(a)
    with pytest.raises(ValueError, match='incompatible'):
        n.add_subscriber(b)
    with b_pub.acquire_batch([1, 2, 3]) as data:
        data.payload[...] = 9
    assert b_pub.drain(timeout=2)
    with b.acquire_batch() as data:
        assert len(data.timestamps) == 3


def test_different_rates_same_batch_duration_and_state(domain):
    _, pair, node = domain
    pa, a = pair('a', rate=100, batch=4)
    pb, b = pair('b', rate=50, batch=2)
    ps, state = pair('state', state=True)
    for pub, times in ((pa, [10, 11, 12, 13]), (pb, [999, 1000])):
        with pub.acquire_batch(times) as data:
            data.payload[...] = 7
        assert pub.drain(timeout=2)
    with ps.acquire(0) as frame:
        frame.payload[...] = 3
    n = node()
    for sub in (a, b, state):
        n.add_subscriber(sub)
    with pytest.raises(ValueError, match='time spans'):
        with n.acquire(timeout=0):
            pass
    assert a.has_frame() and b.has_frame()
    with n.acquire_batch(timeout=1) as data:
        assert isinstance(data[a], Batch) and data[a].payload.shape == (4, 2)
        assert data[b].timestamps == (999, 1000)  # No timestamp synchronization.
        assert isinstance(data[state], Frame)
    assert not a.has_frame() and not b.has_frame()
    with state._endpoint.acquire_batch() as frame:  # Same State shape in transport API.
        assert frame.payload.shape == (2,)


def test_node_batch_outputs_and_manual_checkout(domain):
    _, pair, node = domain
    pa, a = pair('a', rate=100, batch=4)
    pb, b = pair('b', rate=50, batch=2)
    ps, state = pair('state', state=True)
    n = node()
    for pub in (pa, pb, ps):
        n.add_publisher(pub)
    n.reserve('publish', timestamps={pa: [0, 1, 2, 3], pb: [0, 2], ps: 3})
    data = n.checkout_batch(timeout=1)
    data[pa].payload[...] = 4
    data[pb].payload[...] = 2
    data[ps].payload[...] = 8
    assert not a.has_frame() and not b.has_frame()
    n.commit()
    assert n.drain(timeout=2)
    for sub, expected in ((a, 4), (b, 2)):
        sub.reserve()
        block = sub.checkout_batch(timeout=1)
        assert block.payload.shape == (expected, 2)
        assert np.all(block.payload == expected)
        sub.abort()
        assert sub.has_frame()
        with sub.acquire_batch() as again:
            assert len(again.timestamps) == expected
    with state.acquire_batch() as frame:
        assert isinstance(frame, Frame) and np.all(frame.payload == 8)


def test_partial_batch_and_unread_remainder_are_not_aligned(domain):
    bus, pair, node = domain
    pa, a = pair('a', rate=100, batch=4)
    pb, b = pair('b', rate=50, batch=2)
    for pub in (pa, pb):
        with pub.acquire(10) as frame:
            frame.payload[...] = 1
        pub.flush()
    n = node()
    n.add_subscriber(a)
    n.add_subscriber(b)
    with n.acquire_batch(timeout=1) as data:
        assert data[a].timestamps == data[b].timestamps == (10,)
    late = bus.subscriber(pa.spec)
    with pa.acquire_batch([1, 2, 3, 4]) as block:
        block.payload[...] = 3
    assert pa.drain(timeout=2)
    try:
        with late.acquire() as frame:
            assert frame.timestamp == 1
        with late.acquire_batch() as tail:
            assert tail.timestamps == (2, 3, 4)
    finally:
        late.close()
    with pb.acquire_batch([1, 3]) as block:
        block.payload[...] = 3
    assert pb.drain(timeout=2)
    with n.acquire_batch(timeout=1) as data:
        assert len(data[a].timestamps) == 4
        assert len(data[b].timestamps) == 2


def test_batch_write_requires_explicit_times_and_no_pending_tail(domain):
    _, pair, node = domain
    pub, sub = pair('x')
    n = node()
    n.add_publisher(pub)
    with pytest.raises(ValueError, match='timestamps'):
        with n.acquire_batch([1, 2]):
            pass
    with n.acquire(1) as data:
        data[pub].payload[...] = 1
    with pytest.raises(ValueError, match='partial'):
        with n.acquire_batch([1, 2, 3, 4]):
            pass
    assert n.drain(timeout=2)
    with sub.acquire_batch() as data:
        assert data.timestamps == (1,)
    with n.acquire_batch([1, 2, 3, 4]) as data:
        data[pub].payload[...] = 2
    assert n.drain(timeout=2)
