import json
import multiprocessing as mp
from pathlib import Path
from threading import Event
from concurrent.futures import ThreadPoolExecutor

import pytest
from sigbus import (Bus, StreamSpec, FIFOPolicy, PollingWait, StreamInUseError,
                    StreamRemovedError, StreamCorruptedError, StreamPermissionError)
from test_transports import make_transport, publish


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def domain(request, tmp_path):
    transport = make_transport(request.param, tmp_path)
    yield transport, StreamSpec('remove', (8,), 'f8')
    transport.remove('remove', force=True)
    transport.close()


def test_remove_guard_and_existing_endpoints(domain):
    transport, spec = domain
    pub, sub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    publish(pub, 1, 1)
    with pytest.raises(StreamInUseError, match='remove'):
        transport.remove(spec.name)
    assert sub.has_frame()
    transport.remove(spec.name, force=True)
    for action in (sub.has_frame, pub.has_pending, pub.flush):
        with pytest.raises(StreamRemovedError):
            action()
    for lease in (pub.acquire(2), sub.acquire()):
        with pytest.raises(StreamRemovedError):
            with lease:
                pass
    pub.close()
    sub.close()
    pub.close()
    newpub, newsub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    publish(newpub, 3, 3)
    with newsub.acquire() as frame:
        assert frame.timestamp == 3
    newpub.close()
    newsub.close()
    transport.remove(spec)
    transport.remove('missing')


@pytest.mark.parametrize('writer', [False, True])
@pytest.mark.parametrize('ap_error', [False, True])
def test_remove_during_lease(domain, writer, ap_error):
    transport, spec = domain
    pub, sub = Bus(transport).publisher(spec), Bus(transport).subscriber(spec)
    publish(pub, 1, 1)
    error = ValueError('AP exception')
    with pytest.raises(ValueError if ap_error else StreamRemovedError) as caught:
        with (pub.acquire(2) if writer else sub.acquire()):
            transport.remove(spec, force=True)
            if ap_error:
                raise error
    if ap_error:
        assert caught.value is error
    pub.close()
    sub.close()


def test_polling_stops_when_removed(domain):
    transport, spec = domain
    sub = Bus(transport, policy=FIFOPolicy(wait=PollingWait(timeout=2))).subscriber(spec)
    attempted = Event()
    original = sub._endpoint.acquire

    def observed():
        attempted.set()
        return original()

    sub._endpoint.acquire = observed
    def read():
        with sub.acquire():
            pass
    with ThreadPoolExecutor() as pool:
        task = pool.submit(read)
        assert attempted.wait(1)
        transport.remove(spec, force=True)
        with pytest.raises(StreamRemovedError):
            task.result(timeout=3)
    sub.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
def test_legacy_removal_without_spec_or_creation(kind, tmp_path):
    transport = make_transport(kind, tmp_path)
    spec = StreamSpec('remove', (8,), 'f8')
    transport.remove('missing')
    assert not list(transport.directory.glob('*/metadata.json'))
    pub = Bus(transport).publisher(spec)
    pub.close()
    path = next(transport.directory.glob('*/metadata.json'))
    state = json.loads(path.read_text())
    state['version'] = 1
    path.write_text(json.dumps(state))
    transport.remove('remove')
    assert not path.exists()
    transport.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
@pytest.mark.parametrize('damage', ['json', 'structure'])
def test_corruption_not_removal(kind, damage, tmp_path):
    transport = make_transport(kind, tmp_path)
    spec = StreamSpec('remove', (8,), 'f8')
    sub = Bus(transport).subscriber(spec)
    path = next(transport.directory.glob('*/metadata.json'))
    original = path.read_text()
    path.write_text('invalid' if damage == 'json' else '{}')
    with pytest.raises(StreamCorruptedError) as caught:
        sub.has_frame()
    assert caught.value.__cause__ is not None
    path.write_text(original)
    sub.close()
    transport.remove(spec)
    transport.close()


def test_permission_and_other_io_are_distinct(tmp_path, monkeypatch):
    transport = make_transport('file', tmp_path)
    spec = StreamSpec('remove', (8,), 'f8')
    sub = Bus(transport).subscriber(spec)
    original = Path.read_text
    error = PermissionError('denied')
    def denied(path, *args, **kwargs):
        if path.name == 'metadata.json':
            raise error
        return original(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'read_text', denied)
        with pytest.raises(StreamPermissionError) as caught:
            sub.has_frame()
        assert caught.value.__cause__ is error
        with pytest.raises(StreamPermissionError):
            transport.remove(spec, force=True)
        error = OSError('disk failure')
        with pytest.raises(OSError) as caught:
            sub.has_frame()
        assert caught.value is error
    sub.close()
    transport.remove(spec)
    transport.close()


def test_missing_payload_is_corruption(tmp_path):
    transport = make_transport('file', tmp_path)
    spec = StreamSpec('remove', (8,), 'f8')
    pub = Bus(transport).publisher(spec)
    pub.close()
    slot = next(transport.directory.glob('*/slot-0.npy'))
    slot.unlink()
    with pytest.raises(StreamCorruptedError):
        Bus(transport).subscriber(spec)
    transport.remove(spec)
    transport.close()


def removed_child(kind, root, conn):
    transport = make_transport(kind, root)
    spec = StreamSpec('remove', (8,), 'f8')
    sub = Bus(transport).subscriber(spec)
    conn.send('ready')
    conn.recv()
    try:
        sub.has_frame()
    except StreamRemovedError:
        conn.send('removed')
    else:
        conn.send('wrong')
    sub.close()
    transport.close()
    conn.close()


@pytest.mark.parametrize('kind', ['file', 'shared_memory'])
@pytest.mark.parametrize('recreate', [False, True])
def test_removed_in_other_process(kind, recreate, tmp_path):
    transport = make_transport(kind, tmp_path)
    spec = StreamSpec('remove', (8,), 'f8')
    ctx = mp.get_context('spawn')
    parent, child = ctx.Pipe()
    process = ctx.Process(target=removed_child, args=(kind, str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(15)
        assert parent.recv() == 'ready'
        with pytest.raises(StreamInUseError):
            transport.remove(spec)
        transport.remove(spec, force=True)
        if recreate:
            pub = Bus(transport).publisher(spec)
            pub.close()
        parent.send('check')
        assert parent.poll(15)
        assert parent.recv() == 'removed'
        process.join(10)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        parent.close()
    transport.remove(spec)
    transport.close()


def test_permission_on_open_is_classified(tmp_path, monkeypatch):
    transport = make_transport('file', tmp_path)
    spec = StreamSpec('remove', (8,), 'f8')
    original = Path.mkdir
    cause = PermissionError('denied')
    def denied(path, *args, **kwargs):
        if path.parent == transport.directory:
            raise cause
        return original(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'mkdir', denied)
        with pytest.raises(StreamPermissionError) as caught:
            Bus(transport).publisher(spec)
        assert caught.value.__cause__ is cause
    transport.close()


def test_open_corrupt_buffer_and_spec_mismatch_are_distinct(tmp_path):
    transport = make_transport('file', tmp_path)
    spec = StreamSpec('remove', (8,), 'f8')
    pub = Bus(transport).publisher(spec)
    pub.close()
    transport.close()
    other = make_transport('file', tmp_path)
    with pytest.raises(ValueError):
        Bus(other).subscriber(StreamSpec('remove', (7,), 'f8'))
    slot = next(other.directory.glob('*/slot-0.npy'))
    slot.write_bytes(b'not a numpy buffer')
    with pytest.raises(StreamCorruptedError) as caught:
        Bus(other).subscriber(spec)
    assert caught.value.__cause__ is not None
    other.remove(spec)
    other.close()
