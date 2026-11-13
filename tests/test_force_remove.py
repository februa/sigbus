import json
from multiprocessing.shared_memory import SharedMemory

import pytest

from sigbus import Bus, SharedMemoryTransport, StreamSpec


def test_force_reclaims_old_version_and_stale_endpoints(tmp_path):
    transport = SharedMemoryTransport('cleanup', directory=tmp_path)
    spec = StreamSpec('audio', (2,), 'f4')
    pub = Bus(transport).publisher(spec)
    pub.close()
    path = next(transport.directory.glob('*/metadata.json'))
    state = json.loads(path.read_text())
    memory_name = state['shared_memory']
    state['version'] = 4
    state['endpoints'] = {'dead-process': {'mode': 'read', 'active': True}}
    path.write_text(json.dumps(state))
    with pytest.raises(RuntimeError):
        transport.remove(spec)
    transport.remove('audio', force=True)
    assert not path.exists()
    assert (path.parent / 'lock').exists()
    with pytest.raises(FileNotFoundError):
        SharedMemory(name=memory_name, track=False)
    transport.remove('audio', force=True)  # idempotent
    pub = Bus(transport).publisher(spec)  # recreate in the same transport
    pub.close()
    transport.remove(spec)
    transport.close()


def test_force_with_spec_and_missing_segment(tmp_path):
    transport = SharedMemoryTransport('cleanup', directory=tmp_path)
    spec = StreamSpec('audio', (2,), 'f4')
    pub = Bus(transport).publisher(spec)
    pub.close()
    path = next(transport.directory.glob('*/metadata.json'))
    state = json.loads(path.read_text())
    memory = SharedMemory(name=state['shared_memory'], track=False)
    memory.unlink()
    memory.close()
    transport.remove(spec, force=True)
    assert not path.exists()
    transport.remove('never-created', force=True)
    transport.close()


def test_normal_guard_and_target_isolation(tmp_path):
    first = SharedMemoryTransport('first', directory=tmp_path)
    second = SharedMemoryTransport('second', directory=tmp_path)
    spec = StreamSpec('audio', (2,), 'f4')
    a = Bus(first).publisher(spec)
    b = Bus(second).publisher(spec)
    with pytest.raises(RuntimeError):
        first.remove(spec)
    a.close()
    first.remove('audio', force=True)
    with b.acquire(1) as frame:
        frame.payload[:] = 1
    b.close()
    second.remove(spec)
    first.close()
    second.close()
