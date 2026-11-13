from operator import index
from ...errors import StreamInUseError, StreamRemovedError
from threading import RLock

from .endpoint import InProcessReadEndpoint, InProcessWriteEndpoint
from ..storage import InProcessStorage, validate_state


class InProcessTransport:
    """An instance defines one communication domain."""
    def __init__(self, slots=3):
        self.slots = index(slots)
        if self.slots < 1:
            raise ValueError("at least one slot are required")
        self._storages = {}
        self._retired = []
        self._lock = RLock()
        self._closed = False

    def _slots_for(self, spec):
        return self.slots if spec.slots is None else spec.slots

    def _create_storage(self, spec):
        return InProcessStorage(spec, self._slots_for(spec))

    def open_stream(self, stream_spec, mode, *, reuse="fifo", no_readers="hold", overflow="drop_oldest", group=None, ack=None):
        if mode not in ("read", "write"):
            raise ValueError("mode must be read or write")
        if (stream_spec.kind == "state") != (reuse == "state"):
            raise ValueError("StateSpec requires StatePolicy; StreamSpec requires a stream policy")
        with self._lock:
            if self._closed:
                raise RuntimeError("transport is closed")
            storage = self._storages.get(stream_spec.name)
            if storage is None:
                storage = self._create_storage(stream_spec)
                self._storages[stream_spec.name] = storage
            with storage.transaction() as state:
                validate_state(state, stream_spec, self._slots_for(stream_spec))
            endpoint_id = storage.attach(mode, reuse=reuse, no_readers=no_readers, overflow=overflow, group=group, ack=ack)
            cls = InProcessWriteEndpoint if mode == "write" else InProcessReadEndpoint
            return cls(storage, endpoint_id, ack=ack)

    @staticmethod
    def _stream_name(stream):
        name = stream if isinstance(stream, str) else stream.name
        if not isinstance(name, str) or not name:
            raise ValueError("stream name must be a nonempty string")
        return name

    def remove(self, stream, *, force=False):
        name = self._stream_name(stream)
        with self._lock:
            if self._closed:
                raise RuntimeError("transport is closed")
            storage = self._storages.get(name)
            if storage is None:
                return
            with storage.transaction() as state:
                if state["endpoints"] and not force:
                    raise StreamInUseError(f"Stream {name!r}: remove: endpoints are connected")
                storage.removed = True
            self._retired.append(self._storages.pop(name))

    def close(self):
        with self._lock:
            if self._closed:
                return
            for storage in self._storages.values():
                storage.check_idle()
            for storage in self._storages.values():
                storage.close()
            for storage in self._retired:
                storage.close()
            self._retired.clear()
            self._storages.clear()
            self._closed = True


InProcessAdapter = InProcessTransport
