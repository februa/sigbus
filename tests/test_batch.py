import multiprocessing as mp
import numpy as np
import pytest

from sigbus import Bus, FIFOPolicy, LatestPolicy, ReadCompletePolicy, StreamSpec
from test_transports import make_transport


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def batch(request, tmp_path):
    transport = make_transport(request.param, tmp_path, 3)
    spec = StreamSpec('batch', (2, 3), 'f4', batch_size=4)
    yield transport, spec
    if request.param != 'inprocess':
        transport.remove(spec)
    transport.close()


def put(pub, i):
    with pub.acquire(i) as frame:
        frame.payload[...] = i
        return frame.payload.ctypes.data


def take(sub, expected, shape=(2, 3)):
    with sub.acquire() as frame:
        assert frame.timestamp == expected
        assert frame.payload.shape == shape
        assert (frame.payload == expected).all()
        return frame.payload.ctypes.data


def test_batch_auto_publish_flush_and_zero_copy(batch):
    transport, spec = batch
    pub, sub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    assert not pub.has_pending()
    pub.flush()
    addresses = []
    for i in range(4):
        addresses.append(put(pub, i))
        assert pub.has_pending() == (i < 3)
        assert sub.has_frame() == (i == 3)
    assert np.diff(addresses).tolist() == [24, 24, 24]
    for i in range(4):
        assert take(sub, i) == addresses[i]
    assert not sub.has_frame()
    put(pub, 4)
    put(pub, 5)
    assert not sub.has_frame()
    pub.flush()
    pub.flush()
    assert not pub.has_pending()
    take(sub, 4)
    take(sub, 5)
    assert not sub.has_frame()
    pub.close()
    sub.close()


def test_abort_and_close_only_discard_own_tail(batch):
    transport, spec = batch
    bus = Bus(transport)
    a, b, sub = bus.publisher(spec), bus.publisher(spec), bus.subscriber(spec)
    put(a, 1)
    with pytest.raises(ValueError):
        with a.acquire(999) as frame:
            assert a.has_pending()
            with pytest.raises(RuntimeError):
                a.flush()
            frame.payload[:] = 999
            raise ValueError
    put(b, 2)
    a.close()
    assert b.has_pending()
    b.flush()
    take(sub, 2)
    assert not sub.has_frame()
    with pytest.raises(KeyboardInterrupt):
        with b.acquire(99):
            assert not b.has_pending()
            raise KeyboardInterrupt
    assert not b.has_pending()
    b.close()
    with pytest.raises(RuntimeError):
        b.has_pending()
    with pytest.raises(RuntimeError):
        b.flush()
    sub.close()


def test_aborted_item_is_retried_without_losing_prefix(batch):
    transport, spec = batch
    pub, sub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    put(pub, 1)
    with pytest.raises(ValueError):
        with pub.acquire(99) as frame:
            frame.payload[:] = 99
            raise ValueError
    put(pub, 2)
    pub.flush()
    take(sub, 1)
    take(sub, 2)
    assert not sub.has_frame()
    pub.close()
    sub.close()


@pytest.mark.parametrize('policy', [FIFOPolicy(overflow='block'), ReadCompletePolicy()])
def test_independent_read_positions_and_late_join(batch, policy):
    transport, spec = batch
    bus = Bus(transport, policy=policy)
    pub, a, b = bus.publisher(spec), bus.subscriber(spec), bus.subscriber(spec)
    for i in range(4):
        put(pub, i)
    take(a, 0)
    late = bus.subscriber(spec)
    for sub in (b, late):
        for i in range(4):
            take(sub, i)
        sub.close()
    if isinstance(policy, ReadCompletePolicy):
        with pytest.raises(BlockingIOError):
            put(pub, 4)
    with pytest.raises(ValueError):
        with a.acquire() as frame:
            assert frame.timestamp == 1
            raise ValueError
    take(a, 2)
    take(a, 3)
    put(pub, 4)
    assert pub.has_pending()
    pub.close()
    assert not a.has_frame()
    a.close()


def test_latest_only_last_frame_and_partial_hidden(batch):
    transport, spec = batch
    bus = Bus(transport, policy=LatestPolicy())
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    for i in range(4):
        put(pub, i)
    take(sub, 3)
    assert not sub.has_frame()
    put(pub, 4)
    assert not sub.has_frame()
    pub.flush()
    take(sub, 4)
    assert not sub.has_frame()
    pub.close()
    sub.close()


def test_drop_whole_batches_but_preserve_leased_slot(batch):
    transport, spec = batch
    pub, sub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    for i in range(4):
        put(pub, i)
    with sub.acquire() as frame:
        assert frame.timestamp == 0
        for i in range(4, 24):
            put(pub, i)
        assert (frame.payload == 0).all()
    for i in [1, 2, 3, *range(16, 24)]:
        take(sub, i)
    assert not sub.has_frame()
    pub.close()
    sub.close()


@pytest.mark.parametrize('kind', ['inprocess', 'file', 'shared_memory'])
@pytest.mark.parametrize('shape,dtype', [((), 'f8'), ((2, 2, 2), '>i4'),
    ((2,), np.dtype([('x', 'f4'), ('y', 'i4', (2,))]))])
def test_shapes_dtypes_and_reopen(kind, shape, dtype, tmp_path):
    transport = make_transport(kind, tmp_path)
    spec = StreamSpec('array', shape, dtype, batch_size=3)
    pub = Bus(transport).publisher(spec)
    expected = np.ones(shape, dtype)
    for i in range(2):
        with pub.acquire(i) as frame:
            assert frame.payload.shape == shape
            frame.payload[...] = expected
    pub.flush()
    pub.close()
    if kind != 'inprocess':
        transport.close()
        transport = make_transport(kind, tmp_path)
    sub = Bus(transport).subscriber(spec)
    for i in range(2):
        with sub.acquire() as frame:
            assert isinstance(frame.payload, np.ndarray)
            assert frame.timestamp == i
            assert frame.payload.shape == shape
            np.testing.assert_array_equal(frame.payload, expected)
    assert not sub.has_frame()
    sub.close()
    if kind != 'inprocess':
        transport.remove(spec)
    transport.close()


def batch_child(kind, root, conn):
    transport = make_transport(kind, root)
    spec = StreamSpec('ipc-batch', (2, 3), 'f4', batch_size=4)
    sub = Bus(transport).subscriber(spec)
    conn.send('joined')
    conn.recv()
    for expected in ((0, 1, 2, 3), (4, 5)):
        with sub.acquire_batch() as group:
            assert group.timestamps == expected
            for values, timestamp in zip(group.payload, expected):
                assert (values == timestamp).all()
    assert not sub.has_frame()
    sub.close()
    transport.close()
    conn.send('read')
    conn.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_cross_process_batch_flush(kind, tmp_path):
    transport = make_transport(kind, tmp_path)
    spec = StreamSpec('ipc-batch', (2, 3), 'f4', batch_size=4)
    pub = Bus(transport).publisher(spec)
    ctx = mp.get_context('spawn')
    parent, child = ctx.Pipe()
    process = ctx.Process(target=batch_child, args=(kind, str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(15)
        assert parent.recv() == 'joined'
        for i in range(6):
            put(pub, i)
        pub.flush()
        parent.send('ready')
        assert parent.poll(15)
        assert parent.recv() == 'read'
        process.join(10)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        parent.close()
        pub.close()
    transport.remove(spec)
    transport.close()


def test_acquire_batch_concat_axes_and_remaining(batch):
    transport, spec = batch
    bus = Bus(transport)
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    expected = np.arange(24, dtype='f4').reshape(4, 2, 3)
    for i, values in enumerate(expected):
        with pub.acquire(i) as frame:
            frame.payload[:] = values
    with sub.acquire() as frame:
        np.testing.assert_array_equal(frame.payload, expected[0])
    with sub.acquire_batch() as group:
        assert group.timestamps == (1, 2, 3)
        np.testing.assert_array_equal(group.payload, expected[1:])
        for axis in (0, 1, -1, -2):
            joined = group.concatenate(axis)
            np.testing.assert_array_equal(joined, np.concatenate(expected[1:], axis=axis))
            assert np.shares_memory(joined, group.payload) == (axis in (0, -2))
            assert not joined.flags.writeable
        assert np.shares_memory(group.concatenate(), group.payload)
        for axis in (2, -3):
            with pytest.raises(ValueError):
                group.concatenate(axis)
        with pytest.raises(TypeError):
            group.concatenate(0.5)
        assert not sub.has_frame()
        with pytest.raises(RuntimeError):
            with sub.acquire_batch():
                pass
        with pytest.raises(RuntimeError):
            sub.close()
    assert not sub.has_frame()
    pub.close()
    sub.close()


@pytest.mark.parametrize('policy', [FIFOPolicy(), ReadCompletePolicy(), LatestPolicy()])
def test_batch_partial_exception_and_independent_readers(batch, policy):
    transport, spec = batch
    bus = Bus(transport, policy=policy)
    pub, a, b = bus.publisher(spec), bus.subscriber(spec), bus.subscriber(spec)
    for i in range(4):
        put(pub, i)
    with pytest.raises(ValueError, match='AP failure'):
        with a.acquire_batch() as group:
            assert group.timestamps == (0, 1, 2, 3)
            assert group.payload.shape == (4, 2, 3)
            raise ValueError('AP failure')
    assert not a.has_frame()
    with b.acquire_batch() as group:
        assert group.timestamps == (0, 1, 2, 3)
    put(pub, 4)
    pub.flush()
    with a.acquire_batch() as group:
        assert group.timestamps == (4,)
        assert group.concatenate().shape == (2, 3)
    pub.close()
    a.close()
    b.close()


def test_batch_lease_protects_whole_slot(batch):
    transport, spec = batch
    bus = Bus(transport)
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    for i in range(4):
        put(pub, i)
    with sub.acquire_batch() as group:
        for i in range(4, 24):
            put(pub, i)
        assert group.timestamps == (0, 1, 2, 3)
        for i in range(4):
            assert (group.payload[i] == i).all()
    with sub.acquire_batch() as group:
        assert group.timestamps == (16, 17, 18, 19)
    with sub.acquire_batch() as group:
        assert group.timestamps == (20, 21, 22, 23)
    pub.close()
    sub.close()


@pytest.mark.parametrize('kind', ['inprocess', 'file', 'shared_memory'])
@pytest.mark.parametrize('shape', [(512,), ()])
def test_batch_vector_and_scalar(kind, shape, tmp_path):
    transport = make_transport(kind, tmp_path)
    spec = StreamSpec('batch-shape', shape, 'f4', batch_size=2)
    bus = Bus(transport)
    pub, sub = bus.publisher(spec), bus.subscriber(spec)
    for i in range(2):
        with pub.acquire(i) as frame:
            frame.payload[...] = i
    with sub.acquire_batch() as group:
        assert group.payload.shape == (2, *shape)
        if shape:
            joined = group.concatenate()
            assert joined.shape == (1024,)
            assert np.shares_memory(joined, group.payload)
            assert (joined[:512] == 0).all()
            assert (joined[512:] == 1).all()
        else:
            with pytest.raises(ValueError):
                group.concatenate()
    pub.close()
    sub.close()
    transport.remove(spec)
    transport.close()
