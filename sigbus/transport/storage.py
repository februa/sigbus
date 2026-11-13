"""Transport-private slot and lease management. Core knows nothing about slots."""
from contextlib import contextmanager
from operator import index
from math import prod
from threading import RLock
from uuid import uuid4
from time import monotonic
from dataclasses import asdict

import numpy as np

from ..stream import Batch, Frame
from ..errors import (NoFrameAvailable, NoWritableSlot, StreamRemovedError,
                      ParticipantExcludedError, AckTimeoutError)


def initial_state(spec, slots):
    return {
        "version": 7, "delivery": None,
        "spec": {"kind": spec.kind, "name": spec.name, "shape": list(spec.shape),
                 "dtype": np.lib.format.dtype_to_descr(spec.dtype),
                 "layout": spec.layout, "batch_size": spec.batch_size},
        "generation": 0, "latest": None, "endpoints": {},
        "slots": [{"generation": 0, "timestamp": 0, "writer": None, "readers": [], "pending": [], "occupied": False, "count": 0, "timestamps": [], "progress": {}, "barrier": [], "group": None}
                  for _ in range(slots)],
    }


def validate_state(state, spec, slots):
    if state["version"] != 7:
        raise ValueError("unsupported storage version; use a new directory/namespace for this storage format")
    saved = state["spec"]
    if (state["version"] != 7 or saved["name"] != spec.name
            or tuple(saved["shape"]) != spec.shape
            or np.lib.format.descr_to_dtype(saved["dtype"]) != spec.dtype
            or saved["kind"] != spec.kind
            or saved["layout"] != spec.layout or saved["batch_size"] != spec.batch_size
            or len(state["slots"]) != slots):
        raise ValueError("stream specification or slot count does not match existing stream")


class Lease:
    """Single-use context. Payload views are valid only inside this context."""
    def __init__(self, storage, endpoint_id, timestamp=None, *, batch=False):
        self.storage = storage
        self.endpoint_id = endpoint_id
        self.timestamp = timestamp
        self.used = False
        self.batch = batch

    def __enter__(self):
        if self.used:
            raise RuntimeError("lease is single-use")
        self.used = True
        self.slot, self.position, timestamp, self.mode, generation = self.storage.reserve(self.endpoint_id, self.timestamp, batch=self.batch)
        try:
            if self.batch:
                start = self.position + 1 - len(timestamp)
                payload = self.storage.arrays[self.slot][start:self.position + 1].view()
            else:
                payload = self.storage.frame_payload(self.slot, self.position)
            if self.mode == "read":
                payload.flags.writeable = False
            self.frame = Batch(timestamp, payload) if self.batch else Frame(timestamp, payload, generation)
            return self.frame
        except BaseException:
            self.storage.release(self.endpoint_id, self.slot, self.position, self.mode, False)
            raise

    def __exit__(self, exc_type, exc, tb):
        try:
            try:
                self.storage.release(self.endpoint_id, self.slot, self.position,
                                     self.mode, exc_type is None)
            except (StreamRemovedError, ParticipantExcludedError, AckTimeoutError):
                # Removal/expiry may already have committed the release. Never abort
                # twice: another writer could now own the same physical slot.
                if exc_type is None:
                    raise
                # Preserve the exception already raised by AP code.
            except BaseException as cleanup_error:
                if exc_type is not None:
                    exc.add_note(f"Lease cleanup failed: {cleanup_error!r}")
                else:
                    if self.mode == "write":
                        try:
                            self.storage.release(self.endpoint_id, self.slot, self.position,
                                                 self.mode, False)
                        except BaseException as abort_error:
                            cleanup_error.add_note(f"Write abort failed: {abort_error!r}")
                    raise
        finally:
            self.frame.payload.flags.writeable = False
            del self.frame
        return False


class StreamStorage:
    def __init__(self):
        self.local_ids = set()
        self.removed = False

    @staticmethod
    def _expire(state):
        changed = False
        now = monotonic()
        for key, endpoint in state["endpoints"].items():
            ack = endpoint["ack"]
            if ack is None or endpoint["status"] != "joined":
                continue
            # Monotonic clocks are shared by local processes, but reset on reboot.
            age = now - endpoint["last_ack"]
            if 0 <= age < ack["timeout"]:
                continue
            endpoint["status"] = "excluded" if ack["on_timeout"] == "exclude" else "timed_out"
            changed = True
            if endpoint["status"] == "excluded":
                for entry in state["slots"]:
                    was_pending = key in entry["pending"]
                    for field in ("pending", "barrier"):
                        if key in entry[field]:
                            entry[field].remove(key)
                    if was_pending and not entry["pending"] and not entry["readers"]:
                        entry["occupied"] = False
                # A pending partial buffer is not an active ndarray lease.
                if endpoint["buffer"] is not None and not endpoint["active"]:
                    entry = state["slots"][endpoint["buffer"]]
                    entry.update(writer=None, count=0, timestamps=[])
                    endpoint["buffer"] = None
        return changed

    @staticmethod
    def _endpoint(state, endpoint_id):
        endpoint = state["endpoints"].get(endpoint_id)
        if endpoint is None:
            raise RuntimeError("endpoint is closed")
        if endpoint["status"] == "excluded":
            raise ParticipantExcludedError("participant excluded after ACK timeout; close and rejoin")
        if endpoint["status"] == "timed_out":
            raise AckTimeoutError("participant ACK timed out; close and rejoin")
        return endpoint

    def ack(self, endpoint_id):
        with self.transaction() as state:
            self._endpoint(state, endpoint_id)["last_ack"] = monotonic()

    def attach(self, mode, *, reuse="fifo", no_readers="hold", overflow="drop_oldest",
               group=None, ack=None):
        from ..policy import _validate_delivery, AckSettings
        _validate_delivery(reuse, no_readers)
        if ack is not None and not isinstance(ack, AckSettings):
            raise TypeError("ack must be AckSettings or None")
        if overflow not in ("drop_oldest", "block"):
            raise ValueError("overflow must be drop_oldest or block")
        if reuse == "sync" and (not isinstance(group, str) or not group):
            raise ValueError("Sync requires a nonempty group")
        delivery = {"reuse": reuse, "no_readers": no_readers,
                    "overflow": overflow, "group": group}
        endpoint_id = uuid4().hex
        with self.transaction() as state:
            if len(state["slots"]) == 1 and reuse == "available":
                raise ValueError("latest delivery requires at least two slots")
            old = state["delivery"]
            def validate_reader(reader_reuse, reader_group, writer):
                if reader_reuse == "sync" and (writer["reuse"] != "sync"
                                               or reader_group != writer["group"]):
                    raise ValueError("Sync subscriber requires a Sync publisher with the same group")
                if reader_reuse == "all_readers" and writer["reuse"] != "all_readers":
                    raise ValueError("legacy ReadComplete subscriber requires a ReadComplete publisher")
            if mode == "read" and old is not None:
                validate_reader(reuse, group, old)
            if mode == "write":
                for reader in state["endpoints"].values():
                    if reader["mode"] == "read" and reader["status"] == "joined":
                        validate_reader(reader["reuse"], reader["group"], delivery)
            # Writer rules govern shared capacity; readers choose their own order.
            if mode == "write":
                if old is not None and old != delivery:
                    raise ValueError("publisher delivery policy mismatch")
                state["delivery"] = delivery
            latest = state["latest"]
            if mode == "read" and reuse == "fifo":
                for entry in state["slots"]:
                    if entry["occupied"] and entry["writer"] is None:
                        entry["pending"].append(endpoint_id)
            if mode == "read" and reuse == "all_readers" and latest is not None:
                # Legacy ReadComplete joins the current generation.
                if not any(slot["writer"] for slot in state["slots"]):
                    state["slots"][latest]["pending"].append(endpoint_id)
                    state["slots"][latest]["barrier"].append(endpoint_id)
            state["endpoints"][endpoint_id] = {
                "mode": mode, "reuse": reuse, "group": group, "cursor": 0,
                "active": False, "waiting_write": False, "buffer": None, "ack": asdict(ack) if ack else None,
                "last_ack": monotonic(), "status": "joined"}
            self.local_ids.add(endpoint_id)
        return endpoint_id

    @contextmanager
    def write_request(self, endpoint_id):
        with self.transaction() as state:
            endpoint = self._endpoint(state, endpoint_id)
            if endpoint["active"] or endpoint["waiting_write"]:
                raise RuntimeError("endpoint already holds or is waiting for a frame")
            endpoint["waiting_write"] = True
        original_error = None
        try:
            yield
        except BaseException as exc:
            original_error = exc
            raise
        finally:
            try:
                with self.transaction() as state:
                    endpoint = state["endpoints"].get(endpoint_id)
                    if endpoint is not None:
                        endpoint["waiting_write"] = False
            except StreamRemovedError:
                pass
            except BaseException as cleanup_error:
                if original_error is not None:
                    original_error.add_note(f"Writer intent cleanup failed: {cleanup_error!r}")
                else:
                    raise

    def acquire_write(self, endpoint_id, timestamp):
        return Lease(self, endpoint_id, index(timestamp))

    def acquire_read(self, endpoint_id, *, batch=False):
        if batch and self.spec.kind == "state":
            raise ValueError("StateSpec does not support acquire_batch")
        return Lease(self, endpoint_id, batch=batch)

    @staticmethod
    def _read_slot(state, endpoint_id, endpoint):
        if endpoint["reuse"] == "state":
            if len(state["slots"]) == 1 and any(
                    e["waiting_write"] and e["status"] == "joined"
                    for e in state["endpoints"].values()):
                return None
            return state["latest"]
        if endpoint["reuse"] in ("fifo", "sync"):
            candidates = [(slot["generation"], i) for i, slot in enumerate(state["slots"])
                          if slot["occupied"] and endpoint_id in slot["pending"]]
            slot = min(candidates)[1] if candidates else None
        else:
            slot = state["latest"]
        if slot is None or state["slots"][slot]["generation"] <= endpoint["cursor"]:
            return None
        return slot

    def has_frame(self, endpoint_id):
        with self.transaction(read_only=True) as state:
            endpoint = self._endpoint(state, endpoint_id)
            if endpoint["mode"] != "read":
                raise RuntimeError("has_frame requires a read endpoint")
            return not endpoint["active"] and self._read_slot(state, endpoint_id, endpoint) is not None

    def reserve(self, endpoint_id, timestamp, *, batch=False):
        with self.transaction() as state:
            endpoint = self._endpoint(state, endpoint_id)
            if endpoint["active"]:
                raise RuntimeError("endpoint already holds a frame")
            mode = endpoint["mode"]
            delivery = state["delivery"]
            if mode == "write" and any(e["status"] == "timed_out" for e in state["endpoints"].values()):
                raise AckTimeoutError("stream participant ACK timed out; close the participant")
            if mode == "write" and endpoint["buffer"] is not None:
                slot = endpoint["buffer"]
            elif mode == "write":
                readers = [e for e in state["endpoints"].values()
                           if e["mode"] == "read" and e["status"] == "joined"]
                sync = delivery["reuse"] == "sync"
                legacy = delivery["reuse"] == "all_readers"
                targets = ([e for e in readers if e["reuse"] == "sync"
                            and e["group"] == delivery["group"]] if sync else readers)
                if delivery["no_readers"] == "wait" and not targets:
                    raise BlockingIOError("waiting for a subscriber")
                if (sync or legacy) and (
                        any(entry["writer"] is not None and
                            state["endpoints"][entry["writer"]]["status"] != "excluded"
                            for entry in state["slots"])
                        or any(entry["barrier"] for entry in state["slots"])
                        or (delivery["no_readers"] == "hold" and
                            any(entry["occupied"] for entry in state["slots"]))):
                    raise BlockingIOError("waiting for synchronous subscribers to return")
                protect_latest = ((delivery["reuse"] == "state" and len(state["slots"]) > 1) or
                                  delivery["reuse"] == "available" or
                                  any(e["reuse"] == "available" for e in readers))
                def reusable(i, entry):
                    return (entry["writer"] is None and not entry["readers"]
                            and not entry["barrier"]
                            and not (protect_latest and i == state["latest"]))
                candidates = [(entry["generation"], i) for i, entry in enumerate(state["slots"])
                              if reusable(i, entry) and not entry["occupied"]]
                if not candidates and delivery["overflow"] == "drop_oldest":
                    candidates = [(entry["generation"], i) for i, entry in enumerate(state["slots"])
                                  if reusable(i, entry)]
                if not candidates:
                    reason = ("all slots are leased" if all(e["writer"] or e["readers"]
                              for e in state["slots"]) else "capacity exhausted by unread or protected frames")
                    raise NoWritableSlot(f"Stream {state['spec']['name']!r}: {reason}; release/consume frames")
                _, slot = min(candidates)
                if legacy or slot == state["latest"]:
                    state["latest"] = None
                state["slots"][slot]["pending"] = []
                state["slots"][slot]["barrier"] = []
                state["slots"][slot]["occupied"] = False
                state["slots"][slot]["writer"] = endpoint_id
                state["slots"][slot]["count"] = 0
                state["slots"][slot]["timestamps"] = []
                state["slots"][slot]["progress"] = {}
                endpoint["buffer"] = slot
            else:
                slot = self._read_slot(state, endpoint_id, endpoint)
                if slot is None:
                    raise NoFrameAvailable(f"Stream {state['spec']['name']!r}: no unread frame is available; "
                                           "publish more data or use PollingWait to wait")
                state["slots"][slot]["readers"].append(endpoint_id)
                entry = state["slots"][slot]
                position = (entry["count"] - 1 if endpoint["reuse"] in ("available", "state")
                            else entry["progress"].get(endpoint_id, 0))
                if batch:
                    start = 0 if endpoint["reuse"] == "available" else position
                    timestamp = tuple(entry["timestamps"][start:entry["count"]])
                    # A batch acknowledges all remaining frames on lease return.
                    position = entry["count"] - 1
                else:
                    timestamp = entry["timestamps"][position]
            if mode == "write":
                position = state["slots"][slot]["count"]
                endpoint["timestamp"] = timestamp
            endpoint["active"] = True
            generation = state["slots"][slot]["generation"] if mode == "read" else None
            return slot, position, timestamp, mode, generation

    def _publish(self, state, endpoint, slot):
        entry = state["slots"][slot]
        state["generation"] += 1
        entry["generation"] = state["generation"]
        entry["timestamp"] = entry["timestamps"][-1]
        entry["writer"] = None
        endpoint["buffer"] = None
        state["latest"] = slot
        delivery = state["delivery"]
        readers = {key: value for key, value in state["endpoints"].items()
                   if value["mode"] == "read" and value["status"] == "joined"}
        entry["pending"] = [key for key, value in readers.items()
                            if value["reuse"] not in ("available", "state") and
                            (value["reuse"] != "sync" or
                             (delivery["reuse"] == "sync" and value["group"] == delivery["group"]))]
        entry["barrier"] = [key for key, value in readers.items()
                            if delivery["reuse"] == "all_readers" or
                            (delivery["reuse"] == "sync" and value["reuse"] == "sync"
                             and value["group"] == delivery["group"])]
        entry["group"] = delivery["group"]
        entry["occupied"] = bool(entry["pending"]) or delivery["no_readers"] == "hold"

    def release(self, endpoint_id, slot, position, mode, success):
        with self.transaction() as state:
            entry = state["slots"][slot]
            endpoint = state["endpoints"][endpoint_id]
            if mode == "write":
                if endpoint["status"] != "joined":
                    entry.update(writer=None, count=0, timestamps=[])
                    endpoint["buffer"] = None
                elif success:
                    if entry["count"] + 1 == state["spec"]["batch_size"]:
                        # Persist payload before changing publication metadata.
                        self.flush_slot(slot)
                    entry["timestamps"].append(endpoint["timestamp"])
                    entry["count"] += 1
                    if entry["count"] == state["spec"]["batch_size"]:
                        self._publish(state, endpoint, slot)
                elif entry["count"] == 0:
                    entry["writer"] = None
                    endpoint["buffer"] = None
            else:
                entry["readers"].remove(endpoint_id)
                if endpoint["reuse"] != "state":
                    entry["progress"][endpoint_id] = position + 1
                if endpoint["reuse"] != "state" and position + 1 == entry["count"]:
                    if endpoint_id in entry["pending"]:
                        entry["pending"].remove(endpoint_id)
                    if endpoint_id in entry["barrier"]:
                        entry["barrier"].remove(endpoint_id)
                    endpoint["cursor"] = entry["generation"]
                if not entry["pending"] and not entry["readers"]:
                    entry["occupied"] = False
            endpoint["active"] = False
        if mode == "write" and endpoint["status"] != "joined" and success:
            self._endpoint(state, endpoint_id)

    @staticmethod
    def _publisher(state, endpoint_id):
        endpoint = StreamStorage._endpoint(state, endpoint_id)
        if endpoint["mode"] != "write":
            raise RuntimeError("operation requires a publisher")
        return endpoint

    def has_pending(self, endpoint_id):
        with self.transaction(read_only=True) as state:
            endpoint = self._publisher(state, endpoint_id)
            slot = endpoint["buffer"]
            return slot is not None and state["slots"][slot]["count"] > 0

    def flush_pending(self, endpoint_id):
        with self.transaction() as state:
            endpoint = self._publisher(state, endpoint_id)
            if any(e["status"] == "timed_out" for e in state["endpoints"].values()):
                raise AckTimeoutError("stream participant ACK timed out; close the participant")
            if endpoint["active"]:
                raise RuntimeError("release the active write lease before flushing")
            slot = endpoint["buffer"]
            if slot is not None and state["slots"][slot]["count"]:
                self.flush_slot(slot)
                self._publish(state, endpoint, slot)

    def frame_payload(self, slot, position):
        # Construct an ndarray even for scalar shape (); indexing would return a scalar.
        return np.ndarray(self.spec.shape, self.spec.dtype, buffer=self.arrays[slot],
                          offset=position * self.frame_bytes)

    def detach(self, endpoint_id):
        try:
            self._detach(endpoint_id)
        except StreamRemovedError:
            self.local_ids.discard(endpoint_id)

    def _detach(self, endpoint_id):
        with self.transaction() as state:
            endpoint = state["endpoints"].get(endpoint_id)
            if endpoint is not None:
                if endpoint["active"]:
                    raise RuntimeError("release the active frame before closing the endpoint")
                if endpoint["buffer"] is not None:
                    entry = state["slots"][endpoint["buffer"]]
                    entry["writer"] = None
                    entry["count"] = 0
                    entry["timestamps"] = []
                for slot in state["slots"]:
                    if endpoint_id in slot["barrier"]:
                        slot["barrier"].remove(endpoint_id)
                    if endpoint_id in slot["pending"]:
                        slot["pending"].remove(endpoint_id)
                        if not slot["pending"] and not slot["readers"]:
                            slot["occupied"] = False
                    slot["progress"].pop(endpoint_id, None)
                del state["endpoints"][endpoint_id]
                self.local_ids.discard(endpoint_id)

    def check_idle(self):
        if not self.local_ids:
            return
        try:
            with self.transaction() as state:
                if self.local_ids.intersection(state["endpoints"]):
                    raise RuntimeError("close all endpoints before closing transport")
        except StreamRemovedError:
            self.local_ids.clear()

    def flush_slot(self, slot):
        pass


class InProcessStorage(StreamStorage):
    def __init__(self, spec, slots):
        super().__init__()
        self.spec = spec
        self.frame_bytes = prod(spec.shape) * spec.dtype.itemsize
        self.state = initial_state(spec, slots)
        self.lock = RLock()
        self.arrays = [np.empty((spec.batch_size, *spec.shape), spec.dtype) for _ in range(slots)]

    @contextmanager
    def transaction(self, *, read_only=False):
        with self.lock:
            if self.removed:
                raise StreamRemovedError(f"Stream {self.spec.name!r}: access: stream was removed")
            self._expire(self.state)
            yield self.state

    def payload(self, slot):
        return self.arrays[slot]

    def close(self):
        pass
