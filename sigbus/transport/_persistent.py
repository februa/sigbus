"""POSIX-backed metadata locking, shared by file and shared-memory adapters."""
from contextlib import contextmanager
from hashlib import sha256
import json
import math
import os
from pathlib import Path
from threading import RLock
from uuid import uuid4

import numpy as np

from .inprocess.adapter import InProcessTransport
from ._integrity import permissions, read_metadata, check_metadata
from ..errors import StreamInUseError, StreamRemovedError, StreamCorruptedError
from .storage import StreamStorage, initial_state, validate_state


class PersistentStorage(StreamStorage):
    def __init__(self, directory, spec, slots, kind):
        # Keep platform-specific synchronization out of Core and InProcess.
        try:
            import fcntl
        except ImportError as exc:
            raise NotImplementedError("persistent transports v1 require POSIX file locks") from exc
        super().__init__()
        self.spec = spec
        self.frame_bytes = math.prod(spec.shape) * spec.dtype.itemsize
        self._fcntl = fcntl
        self.directory = directory
        self.kind = kind
        self.lock = RLock()
        self._closed = False
        self.arrays = []
        self.memory = None
        self.identity = None
        with permissions(spec.name, "open"):
            self.lock_file = open(directory / "lock", "a+b")
        try:
            with self._locked():
                path = directory / "metadata.json"
                if path.exists():
                    state = read_metadata(path, spec.name, "open")
                    check_metadata(state, spec.name, "open")
                    validate_state(state, spec, slots)
                    if state["kind"] != kind:
                        raise ValueError("transport kind mismatch")
                    try:
                        self._open_payloads(state, spec, slots)
                    except (FileNotFoundError, ValueError, EOFError) as exc:
                        raise StreamCorruptedError(f"Stream {spec.name!r}: open: invalid payload buffer") from exc
                else:
                    state = initial_state(spec, slots)
                    state.update(kind=kind, identity=uuid4().hex)
                    try:
                        self._create_payloads(state, spec, slots)
                        self._save(state)
                    except BaseException:
                        if self.memory is not None:
                            self.memory.unlink()
                        raise
                self.identity = state["identity"]
        except BaseException:
            self.close()
            raise

    @contextmanager
    def _locked(self):
        with self.lock, permissions(self.spec.name, "access"):
            if self._closed:
                raise RuntimeError("storage is closed")
            self._fcntl.flock(self.lock_file.fileno(), self._fcntl.LOCK_EX)
            try:
                yield
            finally:
                self._fcntl.flock(self.lock_file.fileno(), self._fcntl.LOCK_UN)

    @contextmanager
    def transaction(self, *, read_only=False):
        with self._locked():
            if self.removed:
                raise StreamRemovedError(f"Stream {self.spec.name!r}: access: stream was removed")
            try:
                state = read_metadata(self.directory / "metadata.json", self.spec.name, "access")
            except FileNotFoundError as exc:
                self.removed = True
                raise StreamRemovedError(f"Stream {self.spec.name!r}: access: stream was removed") from exc
            check_metadata(state, self.spec.name, "access")
            if state["identity"] != self.identity:
                self.removed = True
                raise StreamRemovedError(f"Stream {self.spec.name!r}: access: stream was replaced")
            if state["version"] != 7:
                raise StreamCorruptedError(f"Stream {self.spec.name!r}: access: storage format changed")
            # Detect missing/truncated mapped files before constructing or reading views.
            if self.kind == "file":
                try:
                    for array in self.arrays:
                        if Path(array.filename).stat().st_size < array.offset + array.nbytes:
                            raise ValueError("truncated slot file")
                except (FileNotFoundError, ValueError) as exc:
                    raise StreamCorruptedError(f"Stream {self.spec.name!r}: access: damaged payload buffer") from exc
            # Persist exclusion even when the following acquisition raises.
            if self._expire(state):
                self._save(state)
            yield state
            if not read_only:
                self._save(state)

    def _save(self, state):
        temp = self.directory / "metadata.tmp"
        temp.write_text(json.dumps(state))
        os.replace(temp, self.directory / "metadata.json")

    def _create_payloads(self, state, spec, slots):
        if self.kind == "file":
            for i in range(slots):
                self.arrays.append(np.lib.format.open_memmap(
                    self.directory / f"slot-{i}.npy", mode="w+",
                    dtype=spec.dtype, shape=(spec.batch_size, *spec.shape)))
        else:
            from multiprocessing.shared_memory import SharedMemory
            size = math.prod(spec.shape) * spec.dtype.itemsize * spec.batch_size
            self.memory = SharedMemory(create=True, size=size * slots, track=False)
            state["shared_memory"] = self.memory.name
            self.arrays = [np.ndarray((spec.batch_size, *spec.shape), spec.dtype, buffer=self.memory.buf,
                                      offset=i * size) for i in range(slots)]

    def _open_payloads(self, state, spec, slots):
        if self.kind == "file":
            for i in range(slots):
                array = np.load(self.directory / f"slot-{i}.npy", mmap_mode="r+", allow_pickle=False)
                self.arrays.append(array)
                if array.shape != (spec.batch_size, *spec.shape) or array.dtype != spec.dtype or not array.flags.c_contiguous:
                    raise ValueError("invalid slot file")
        else:
            from multiprocessing.shared_memory import SharedMemory
            size = math.prod(spec.shape) * spec.dtype.itemsize * spec.batch_size
            self.memory = SharedMemory(name=state["shared_memory"], track=False)
            # macOS may round allocations up to a page boundary.
            if self.memory.size < size * slots:
                raise ValueError("invalid shared-memory segment size")
            self.arrays = [np.ndarray((spec.batch_size, *spec.shape), spec.dtype, buffer=self.memory.buf,
                                      offset=i * size) for i in range(slots)]

    def payload(self, slot):
        return self.arrays[slot]

    def flush_slot(self, slot):
        if self.kind == "file":
            self.arrays[slot].flush()

    def close(self):
        if self._closed:
            return
        self.arrays.clear()
        if self.memory is not None:
            self.memory.close()
        self.lock_file.close()
        self._closed = True


class PersistentTransport(InProcessTransport):
    kind = None

    def __init__(self, directory, slots=3):
        super().__init__(slots)
        self.directory = Path(directory).resolve()
        with permissions(str(self.directory), "create transport"):
            self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    def _stream_directory(self, name):
        return self.directory / sha256(name.encode()).hexdigest()

    def _create_storage(self, spec):
        directory = self._stream_directory(spec.name)
        with permissions(spec.name, "open"):
            directory.mkdir(exist_ok=True, mode=0o700)
            return PersistentStorage(directory, spec, self._slots_for(spec), self.kind)

    def remove(self, stream, *, force=False):
        import fcntl
        name = self._stream_name(stream)
        directory = self._stream_directory(name)
        with self._lock, permissions(name, "remove"):
            if self._closed:
                raise RuntimeError("transport is closed")
            try:
                lock = (directory / "lock").open("a+b")
            except FileNotFoundError:
                return
            with lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                try:
                    path = directory / "metadata.json"
                    try:
                        state = read_metadata(path, name, "remove")
                    except FileNotFoundError:
                        state = None
                    if state is not None:
                        check_metadata(state, name, "remove", legacy=True)
                        if state["kind"] != self.kind:
                            raise StreamCorruptedError(f"Stream {name!r}: remove: transport kind mismatch")
                        if state["endpoints"] and not force:
                            raise StreamInUseError(f"Stream {name!r}: remove: endpoints are connected")
                        if self.kind == "shared_memory":
                            from multiprocessing.shared_memory import SharedMemory
                            memory_name = state.get("shared_memory")
                            if not isinstance(memory_name, str) or not memory_name:
                                raise StreamCorruptedError(f"Stream {name!r}: remove: missing shared-memory name")
                            try:
                                memory = SharedMemory(name=memory_name, track=False)
                            except FileNotFoundError:
                                pass
                            else:
                                try:
                                    memory.unlink()
                                except FileNotFoundError:
                                    pass
                                finally:
                                    memory.close()
                        else:
                            for i in range(len(state["slots"])):
                                (directory / f"slot-{i}.npy").unlink(missing_ok=True)
                        path.unlink()
                    storage = self._storages.pop(name, None)
                    if storage is not None:
                        storage.removed = True
                        self._retired.append(storage)
                    # Preserve lock inode so remove/recreate use the same lock.
                finally:
                    fcntl.flock(lock, fcntl.LOCK_UN)
