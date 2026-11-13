from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import multiprocessing as mp
import subprocess
import sys

import numpy as np
import pytest

from sigbus import LatestPolicy, Bus, FileTransport, InProcessTransport, SharedMemoryTransport, StreamSpec


def make_transport(kind, root, slots=3):
    if kind == "inprocess":
        return InProcessTransport(slots)
    if kind == "file":
        return FileTransport(root, slots)
    return SharedMemoryTransport("tests", slots, directory=root)


@pytest.fixture(params=["inprocess", "file", "shared_memory"])
def domain(request, tmp_path):
    transport = make_transport(request.param, tmp_path)
    spec = StreamSpec("signal", (8,), np.float64)
    yield transport, spec
    # Tests close their endpoints; persistent data is removed explicitly.
    if request.param != "inprocess":
        transport.remove(spec)
    transport.close()


def publish(pub, timestamp, value):
    with pub.acquire(timestamp) as frame:
        frame.payload[:] = value


def test_common_api_and_independent_subscribers(domain):
    transport, spec = domain
    bus = Bus(transport, policy=LatestPolicy())
    pub, sub1, sub2 = bus.publisher(spec), bus.subscriber(spec), bus.subscriber(spec)
    with pytest.raises(BlockingIOError):
        with sub1.acquire():
            pass
    publish(pub, 42, np.arange(8))
    for sub in (sub1, sub2):
        with sub.acquire() as frame:
            assert frame.timestamp == 42
            np.testing.assert_array_equal(frame.payload, np.arange(8))
            assert not frame.payload.flags.writeable
            with pytest.raises(ValueError):
                frame.payload[0] = 1
        with pytest.raises(BlockingIOError):
            with sub.acquire():
                pass
    for endpoint in (pub, sub1, sub2):
        endpoint.close()
        endpoint.close()
        with pytest.raises(RuntimeError):
            with endpoint.acquire(0) if endpoint is pub else endpoint.acquire():
                pass


def test_pinned_slots_and_backpressure(domain):
    transport, spec = domain
    bus = Bus(transport, policy=LatestPolicy())
    pub = bus.publisher(spec)
    subscribers = [bus.subscriber(spec) for _ in range(3)]
    with ExitStack() as stack:
        frames = []
        for generation, sub in enumerate(subscribers):
            publish(pub, generation, generation)
            frames.append(stack.enter_context(sub.acquire()))
        with pytest.raises(BlockingIOError):
            publish(pub, 3, 3)
        for generation, frame in enumerate(frames):
            assert frame.timestamp == generation
            np.testing.assert_array_equal(frame.payload, np.full(8, generation))
    publish(pub, 4, 4)
    pub.close()
    for sub in subscribers:
        sub.close()


def test_exception_rollback_and_latest_semantics(domain):
    transport, spec = domain
    pub, sub = Bus(transport, policy=LatestPolicy()).publisher(spec), Bus(transport, policy=LatestPolicy()).subscriber(spec)
    publish(pub, 1, 1)
    with pytest.raises(ValueError):
        with pub.acquire(2) as frame:
            frame.payload[:] = 999
            raise ValueError("abort")
    with sub.acquire() as frame:
        assert frame.timestamp == 1
        assert np.all(frame.payload == 1)
    publish(pub, 3, 3)
    publish(pub, 4, 4)
    with pytest.raises(ValueError):
        with sub.acquire() as frame:
            assert frame.timestamp == 4
            raise ValueError("processing failed")
    with pytest.raises(BlockingIOError):
        with sub.acquire():
            pass
    pub.close()
    sub.close()


def test_close_while_active_and_nested_acquire(domain):
    transport, spec = domain
    pub = Bus(transport, policy=LatestPolicy()).publisher(spec)
    lease = pub.acquire(1)
    with lease as frame:
        frame.payload[:] = 5
        with pytest.raises(RuntimeError):
            pub.close()
        with pytest.raises(RuntimeError):
            transport.close()
        with pytest.raises(RuntimeError):
            with pub.acquire(2):
                pass
    with pytest.raises(RuntimeError):
        with lease:
            pass
    sub = Bus(transport, policy=LatestPolicy()).subscriber(spec)
    with sub.acquire():
        with pytest.raises(RuntimeError):
            sub.close()
        with pytest.raises(RuntimeError):
            with sub.acquire():
                pass
    pub.close()
    sub.close()


def test_spec_conflict_and_domain_isolation(domain):
    transport, spec = domain
    pub = Bus(transport, policy=LatestPolicy()).publisher(spec)
    for other in (StreamSpec(spec.name, (4,), np.float64), StreamSpec(spec.name, (8,), np.int64)):
        with pytest.raises(ValueError):
            Bus(transport, policy=LatestPolicy()).subscriber(other)
    with pytest.raises(ValueError):
        transport.open_stream(spec, "invalid")
    second = StreamSpec("other", (8,), np.float64)
    sub = Bus(transport, policy=LatestPolicy()).subscriber(second)
    publish(pub, 1, 1)
    with pytest.raises(BlockingIOError):
        with sub.acquire():
            pass
    pub.close()
    sub.close()
    if hasattr(transport, "remove"):
        transport.remove(second)


def test_zero_copy_and_multiple_reader_pins(domain):
    transport, spec = domain
    bus = Bus(transport, policy=LatestPolicy())
    pub, sub1, sub2 = bus.publisher(spec), bus.subscriber(spec), bus.subscriber(spec)
    with pub.acquire(1) as write:
        write.payload[:] = 1
        address = write.payload.ctypes.data
    with sub1.acquire() as read1:
        with sub2.acquire() as read2:
            assert address == read1.payload.ctypes.data == read2.payload.ctypes.data
        for i in range(2, 8):
            publish(pub, i, i)
        assert np.all(read1.payload == 1)
    for endpoint in (pub, sub1, sub2):
        endpoint.close()


@pytest.mark.parametrize("kind", ["file", "shared_memory"])
def test_reopen_and_independent_instances(kind, tmp_path):
    spec = StreamSpec("signal" if kind == "file" else "a/../signal", (3,), np.int32)
    first = make_transport(kind, tmp_path)
    pub = Bus(first, policy=LatestPolicy()).publisher(spec)
    publish(pub, 11, [3, 4, 5])
    pub.close()
    first.close()
    second = make_transport(kind, tmp_path)
    third = make_transport(kind, tmp_path)
    sub = Bus(second, policy=LatestPolicy()).subscriber(spec)
    pub = Bus(third, policy=LatestPolicy()).publisher(spec)
    with sub.acquire() as frame:
        assert frame.timestamp == 11
        np.testing.assert_array_equal(frame.payload, [3, 4, 5])
        with pytest.raises(RuntimeError):
            third.remove(spec)
    sub.close()
    second.close()  # Other transport's endpoint does not prevent local close.
    publish(pub, 12, 12)
    pub.close()
    third.remove(spec)
    third.close()


def child_reader(kind, root, conn):
    transport = make_transport(kind, root)
    spec = StreamSpec("cross-process", (128,), np.float64)
    sub = Bus(transport, policy=LatestPolicy()).subscriber(spec)
    with sub.acquire() as frame:
        conn.send((frame.timestamp, bool(np.all(frame.payload == 1))))
        conn.recv()
        conn.send(bool(np.all(frame.payload == 1)))
    sub.close()
    transport.close()
    conn.close()


@pytest.mark.parametrize("kind", ["file", "shared_memory"])
def test_spawn_reader_holds_frame_during_parent_writes(kind, tmp_path):
    transport = make_transport(kind, tmp_path)
    spec = StreamSpec("cross-process", (128,), np.float64)
    pub = Bus(transport, policy=LatestPolicy()).publisher(spec)
    publish(pub, 1, 1)
    ctx = mp.get_context("spawn")
    parent, child = ctx.Pipe()
    process = ctx.Process(target=child_reader, args=(kind, str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(15)
        assert parent.recv() == (1, True)
        for value in range(2, 20):
            publish(pub, value, value)
        parent.send("done")
        assert parent.poll(15)
        assert parent.recv() is True
        process.join(15)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        parent.close()
        pub.close()
    transport.remove(spec)
    transport.close()


def test_unrelated_shared_memory_process_exit_preserves_segment(tmp_path):
    transport = make_transport("shared_memory", tmp_path)
    spec = StreamSpec("signal", (8,), np.float64)
    pub = Bus(transport, policy=LatestPolicy()).publisher(spec)
    code = '''
import sys
import numpy as np
from sigbus import LatestPolicy, Bus, SharedMemoryTransport, StreamSpec
t = SharedMemoryTransport("tests", directory=sys.argv[1])
p = Bus(t, policy=LatestPolicy()).publisher(StreamSpec("signal", (8,), np.float64))
with p.acquire(9) as f:
    f.payload[:] = 9
p.close()
t.close()
'''
    result = subprocess.run([sys.executable, "-c", code, str(tmp_path)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert not result.stderr
    sub = Bus(transport, policy=LatestPolicy()).subscriber(spec)
    with sub.acquire() as frame:
        assert frame.timestamp == 9
        assert np.all(frame.payload == 9)
    pub.close()
    sub.close()
    transport.remove(spec)
    transport.close()


@pytest.mark.parametrize("kind", ["inprocess", "file", "shared_memory"])
@pytest.mark.parametrize("shape,dtype", [((), "float32"), ((2, 3), ">i4"),
                                         ((2,), np.dtype([("x", "f8"), ("v", "i4", (2,))]))])
def test_numpy_shapes_and_dtypes(kind, shape, dtype, tmp_path):
    spec = StreamSpec("numpy", shape, dtype)
    transport = make_transport(kind, tmp_path)
    pub, sub = Bus(transport, policy=LatestPolicy()).publisher(spec), Bus(transport, policy=LatestPolicy()).subscriber(spec)
    expected = np.ones(shape, dtype)
    with pub.acquire(123) as frame:
        frame.payload[...] = expected
    with sub.acquire() as frame:
        assert frame.payload.dtype == expected.dtype
        assert frame.payload.shape == expected.shape
        np.testing.assert_array_equal(frame.payload, expected)
    pub.close()
    sub.close()
    if kind != "inprocess":
        transport.close()
        transport = make_transport(kind, tmp_path)
        sub = Bus(transport, policy=LatestPolicy()).subscriber(spec)
        with sub.acquire() as frame:
            np.testing.assert_array_equal(frame.payload, expected)
        sub.close()
        transport.remove(spec)
    transport.close()


def test_threaded_publishers_are_atomic():
    transport = InProcessTransport(slots=8)
    spec = StreamSpec("threads", (1024,), np.int64)
    publishers = [Bus(transport, policy=LatestPolicy()).publisher(spec) for _ in range(4)]
    sub = Bus(transport, policy=LatestPolicy()).subscriber(spec)

    def worker(item):
        i, pub = item
        for j in range(50):
            publish(pub, i * 1000 + j, i * 1000 + j)

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker, item) for item in enumerate(publishers)]
        for _ in range(100):
            try:
                with sub.acquire() as frame:
                    assert np.all(frame.payload == frame.timestamp)
            except BlockingIOError:
                pass
        for future in futures:
            future.result()
    for pub in publishers:
        pub.close()
    sub.close()
    transport.close()


@pytest.mark.parametrize("kwargs", [{"name": ""}, {"shape": (0,)}, {"shape": (-1,)},
                                    {"dtype": object}, {"dtype": "V0"},
                                    {"layout": "column_major"}, {"batch_size": 0}])
def test_invalid_spec(kwargs):
    fields = dict(name="x", shape=(2,), dtype="f8")
    fields.update(kwargs)
    with pytest.raises(ValueError):
        StreamSpec(**fields)


def test_public_exports_and_slot_validation():
    namespace = {}
    exec("from sigbus import *", namespace)
    assert namespace["Bus"] is Bus
    with pytest.raises(ValueError):
        InProcessTransport(slots=0)
    transport = InProcessTransport()
    transport.close()
    transport.close()
    with pytest.raises(RuntimeError):
        Bus(transport, policy=LatestPolicy()).publisher(StreamSpec("x", (2,), "f8"))


def child_writer(kind, root, number, conn):
    transport = make_transport(kind, root, slots=6)
    spec = StreamSpec("parallel", (2048,), np.int64)
    pub = Bus(transport, policy=LatestPolicy()).publisher(spec)
    conn.send("ready")
    conn.recv()
    for counter in range(40):
        value = number * 1000 + counter
        publish(pub, value, value)
    pub.close()
    transport.close()
    conn.send("done")
    conn.close()


@pytest.mark.parametrize("kind", ["file", "shared_memory"])
def test_concurrent_process_writers(kind, tmp_path):
    import time
    import json

    transport = make_transport(kind, tmp_path, slots=6)
    spec = StreamSpec("parallel", (2048,), np.int64)
    sub = Bus(transport, policy=LatestPolicy()).subscriber(spec)
    ctx = mp.get_context("spawn")
    processes, connections = [], []
    try:
        for number in range(2):
            parent, child = ctx.Pipe()
            process = ctx.Process(target=child_writer, args=(kind, str(tmp_path), number, child))
            process.start()
            child.close()
            processes.append(process)
            connections.append(parent)
        for connection in connections:
            assert connection.poll(15)
            assert connection.recv() == "ready"
        for connection in connections:
            connection.send("start")
        deadline = time.monotonic() + 20
        received = 0
        while any(process.is_alive() for process in processes):
            assert time.monotonic() < deadline
            try:
                with sub.acquire() as frame:
                    assert np.all(frame.payload == frame.timestamp)
                    received += 1
            except BlockingIOError:
                time.sleep(0.001)
        for process, connection in zip(processes, connections):
            process.join(5)
            assert process.exitcode == 0
            assert connection.poll(5)
            assert connection.recv() == "done"
        assert received > 0
        metadata = list(transport.directory.glob("*/metadata.json"))
        assert len(metadata) == 1
        state = json.loads(metadata[0].read_text())
        assert state["generation"] == 80
        assert state["slots"][state["latest"]]["generation"] == 80
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(5)
        for connection in connections:
            connection.close()
        sub.close()
    transport.remove(spec)
    transport.close()


def test_file_format_generations_and_numpy_compatibility(tmp_path):
    import json

    spec = StreamSpec("saved", (4,), np.float32)
    transport = FileTransport(tmp_path)
    pub = Bus(transport, policy=LatestPolicy()).publisher(spec)
    for generation in range(10):
        publish(pub, generation, generation)
    pub.close()
    transport.close()
    metadata = next(tmp_path.glob("*/metadata.json"))
    state = json.loads(metadata.read_text())
    assert state["generation"] == 10
    assert len(list(metadata.parent.glob("slot-*.npy"))) == 3
    array = np.load(metadata.parent / f'slot-{state["latest"]}.npy', allow_pickle=False)
    np.testing.assert_array_equal(array, np.full((1, 4), 9, np.float32))
    transport = FileTransport(tmp_path)
    pub = Bus(transport, policy=LatestPolicy()).publisher(spec)
    publish(pub, 10, 10)
    pub.close()
    assert json.loads(metadata.read_text())["generation"] == 11
    transport.remove(spec)
    transport.close()
