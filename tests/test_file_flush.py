import json
import numpy as np
import pytest

from sigbus import Bus, FileTransport, StreamSpec


def test_flush_only_published_slot_before_metadata(tmp_path, monkeypatch):
    transport = FileTransport(tmp_path, slots=10)
    spec = StreamSpec('flush', (2,), 'f4', batch_size=100)
    pub, sub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    storage = pub._endpoint._storage
    calls = []
    original = np.memmap.flush

    def observed(array):
        slot = next(i for i, item in enumerate(storage.arrays) if item is array)
        state = json.loads((storage.directory / 'metadata.json').read_text())
        calls.append((slot, state['generation']))
        return original(array)

    monkeypatch.setattr(np.memmap, 'flush', observed)
    for i in range(100):
        with pub.acquire(i) as frame:
            frame.payload[:] = i
        if i < 99:
            assert calls == []
    assert calls == [(0, 0)]
    with pub.acquire(100) as frame:
        frame.payload[:] = 100
    assert calls == [(0, 0)]
    pub.flush()
    assert calls == [(0, 0), (1, 1)]
    pub.flush()
    with pytest.raises(ValueError):
        with pub.acquire(101):
            raise ValueError
    with pub.acquire(102) as frame:
        frame.payload[:] = 102
    pub.close()  # Discarding the tail must not explicitly flush it.
    assert calls == [(0, 0), (1, 1)]
    sub.close()
    transport.remove(spec)
    transport.close()


@pytest.mark.parametrize('batch_size,explicit', [(1, False), (3, False), (3, True)])
def test_flush_failure_preserves_prefix_and_releases_lease(tmp_path, monkeypatch, batch_size, explicit):
    transport = FileTransport(tmp_path)
    spec = StreamSpec('failure', (2,), 'f4', batch_size=batch_size)
    pub, sub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    storage = pub._endpoint._storage
    for i in range(batch_size - 1):
        with pub.acquire(i) as frame:
            frame.payload[:] = i
    original = storage.flush_slot

    def fail(slot):
        raise OSError('disk failure')

    monkeypatch.setattr(storage, 'flush_slot', fail)
    with pytest.raises(OSError, match='disk failure'):
        if explicit:
            pub.flush()
        else:
            with pub.acquire(batch_size - 1) as frame:
                frame.payload[:] = 999
    assert not sub.has_frame()
    assert pub.has_pending() == (batch_size > 1)
    monkeypatch.setattr(storage, 'flush_slot', original)
    if explicit:
        pub.flush()
        count = batch_size - 1
    else:
        with pub.acquire(batch_size - 1) as frame:
            frame.payload[:] = batch_size - 1
        count = batch_size
    for i in range(count):
        with sub.acquire() as frame:
            assert frame.timestamp == i
            assert (frame.payload == i).all()
    assert not sub.has_frame()
    pub.close()
    sub.close()
    transport.remove(spec)
    transport.close()
