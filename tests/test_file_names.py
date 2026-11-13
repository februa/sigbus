from hashlib import sha256

import pytest

from sigbus import Bus, FileTransport, StreamSpec


def test_readable_directory_and_reopen(tmp_path):
    spec = StreamSpec('audio_1-test', (2,), 'f4')
    transport = FileTransport(tmp_path)
    pub = Bus(transport).publisher(spec)
    with pub.acquire(123) as frame:
        frame.payload[:] = [1, 2]
    pub.close()
    transport.close()
    assert (tmp_path / spec.name / 'metadata.json').is_file()
    assert (tmp_path / spec.name / 'slot-0.npy').is_file()
    transport = FileTransport(tmp_path)
    sub = Bus(transport).subscriber(spec)
    with sub.acquire() as frame:
        assert frame.timestamp == 123
        assert frame.payload.tolist() == [1, 2]
    sub.close()
    transport.remove(spec.name)
    assert not (tmp_path / spec.name / 'metadata.json').exists()
    transport.close()


@pytest.mark.parametrize('name', ['../escape', '/absolute', '.', '..', 'Audio',
                                  '音声', 'a b', 'a.b', '_audio', 'a' * 64])
def test_invalid_file_name_has_no_side_effects(tmp_path, name):
    transport = FileTransport(tmp_path)
    spec = StreamSpec(name, (2,), 'f4')
    with pytest.raises(ValueError):
        Bus(transport).publisher(spec)
    with pytest.raises(ValueError):
        transport.remove(name, force=True)
    assert list(tmp_path.iterdir()) == []
    transport.close()


def test_legacy_directory_requires_explicit_migration(tmp_path):
    spec = StreamSpec('audio', (2,), 'f4')
    legacy = tmp_path / sha256(spec.name.encode()).hexdigest()
    legacy.mkdir()
    transport = FileTransport(tmp_path)
    with pytest.raises(ValueError, match='legacy hashed'):
        Bus(transport).publisher(spec)
    with pytest.raises(ValueError, match='legacy hashed'):
        transport.remove(spec, force=True)
    assert legacy.exists()
    assert not (tmp_path / 'audio').exists()
    transport.close()
