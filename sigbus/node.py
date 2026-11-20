"""Grouped bus participation: reservation, AP access, commit, and deferred send."""
from collections import deque
from collections.abc import Mapping
from fractions import Fraction
from contextlib import ExitStack, contextmanager
import math
import sys
from operator import index
from threading import Condition, Event, Thread, local
from time import monotonic
from types import MappingProxyType

from .bus import Publisher, Subscriber
from .errors import AckTimeoutError, NodeSendError, StreamRemovedError
from .policy import StatePolicy


def _deadline(timeout):
    if timeout is not None and (not math.isfinite(timeout) or timeout < 0):
        raise ValueError("timeout must be finite and nonnegative")
    return None if timeout is None else monotonic() + timeout


class _Lifecycle:
    """Shared lifecycle for one endpoint or a group, including multi-pass work.

    Registration transfers endpoint resource ownership; close closes them. Reservations progress independently, retaining successful pins.
    No cross-stream atomic visibility, durable outbox, or fairness is promised.
    """

    def __init__(self, *, interval=0.001, stop_event=None):
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("interval must be finite and positive")
        self.interval = interval
        self.stop_event = stop_event
        self._draining = False
        self._flushing = False
        self._inputs = []
        self._outputs = []
        self._condition = Condition()
        self._stop = Event()
        self._send_stop = Event()
        self._queue = deque()
        self._unreleased = []
        self._request = None
        self._checkout_mode = None
        self._reservations = {}
        self._frames = {}
        self._checkout_running = False
        self._with_active = False
        self._calls = local()
        self._worker = None
        self._error = None
        self._busy = False
        self._processing = False
        self._started = False
        self._closing = False
        self._closed = False
        self._resource_owner = self

    def _add(self, endpoint, cls, collection):
        if not isinstance(endpoint, cls):
            raise TypeError(f"expected {cls.__name__}")
        with self._condition, endpoint._ownership_lock:
            self._check()
            if self._started:
                raise RuntimeError("register endpoints before the first acquisition")
            endpoint._check_node()
            raw = endpoint._endpoint
            if not all(callable(getattr(raw, method, None)) for method in
                       ("reserve", "check_idle", "queue_pending", "discard_pending")):
                raise TypeError("transport does not support Node reservations")
            candidates = self._inputs + self._outputs + [endpoint]
            if not (self._rates_match(candidates, False) or self._rates_match(candidates, True)):
                raise ValueError("Stream rates are incompatible in both Frame and Batch modes")
            # Refuse to claim an endpoint with a lease or an existing partial batch.
            endpoint._prepare_transfer()
            endpoint._resources.transfer(endpoint, self)
            collection.append(endpoint)
        return endpoint

    def add_publisher(self, publisher):
        return self._add(publisher, Publisher, self._outputs)

    def add_subscriber(self, subscriber):
        return self._add(subscriber, Subscriber, self._inputs)

    def _check(self, *, closing=False):
        if self._closed or (self._closing and not closing):
            raise RuntimeError("Node is closed or closing")
        if self._error is not None:
            raise NodeSendError("Node publication stopped; discard() to discard pending output") from self._error

    def stop(self):
        """Cancel acquisition waits. Committed sends continue until drained/closed."""
        self._stop.set()
        with self._condition:
            self._condition.notify_all()

    def _cancelled(self, stop_event=None):
        return ((self.stop_event is not None and self.stop_event.is_set()) or
                (stop_event is not None and stop_event.is_set()))

    def _check_wait(self, deadline, stop_event):
        self._check()
        if self._stop.is_set() or self._cancelled(stop_event):
            raise InterruptedError("Node acquisition cancelled")
        if deadline is not None and monotonic() >= deadline:
            raise TimeoutError("Node acquisition timed out")

    def _manual_check(self):
        if self._with_active and not getattr(self._calls, "wrapper", False):
            raise RuntimeError("manual operations are not allowed inside acquire()")
        if self._checkout_running:
            raise RuntimeError("checkout is already running")

    def _refresh(self):
        self._busy = bool(self._request or self._reservations)
        self._processing = bool(self._frames)
        self._condition.notify_all()

    @staticmethod
    def _rates_match(endpoints, batch):
        spans = [Fraction(e.spec.batch_size if batch else 1) / Fraction(str(e.spec.rate_hz))
                 for e in endpoints if e.spec.kind == "stream"]
        return not spans or all(span == spans[0] for span in spans)

    def reserve(self, target="all", *, timestamp=None, timestamps=None):
        """Register a request: all, subscribe, or publish. checkout obtains access."""
        if target not in ("all", "subscribe", "publish"):
            raise ValueError("target must be all, subscribe, or publish")
        if timestamps is not None:
            if timestamp is not None:
                raise ValueError("specify timestamp or timestamps, not both")
            timestamp = timestamps
        if timestamp is not None and not isinstance(timestamp, (Mapping, tuple, list)):
            timestamp = index(timestamp)
        with self._condition:
            self._manual_check()
            self._check()
            if self._flushing or self._request is not None:
                raise RuntimeError("finish the pending reservation/drain first")
            targets = ((self._inputs if target != "publish" else []) +
                       (self._outputs if target != "subscribe" else []))
            if not targets:
                raise ValueError("no endpoints registered for this target")
            if any(e in self._reservations for e in targets):
                raise RuntimeError("target already reserved or checked out")
            if (timestamp is None and any(e in self._outputs for e in targets)
                    and not any(e in targets or e in self._reservations for e in self._inputs)):
                raise ValueError("output-only reservation requires a timestamp")
            self._request = (targets, timestamp)
            self._checkout_mode = None
            self._started = True
            self._refresh()

    def checkout(self, *, timeout=None, stop_event=None):
        return self._checkout(batch=False, timeout=timeout, stop_event=stop_event)

    def checkout_batch(self, *, timeout=None, stop_event=None):
        return self._checkout(batch=True, timeout=timeout, stop_event=stop_event)

    def _checkout(self, *, batch, timeout, stop_event):
        """Obtain reserved regions. On timeout retain pins for retry or abort."""
        deadline = _deadline(timeout)
        with self._condition:
            self._manual_check()
            self._check()
            if self._request is None:
                raise RuntimeError("reserve before checkout")
            targets, timestamp = self._request
            if not self._rates_match(self._inputs + self._outputs, batch):
                raise ValueError("incompatible Stream time spans for selected acquisition mode")
            if self._checkout_mode is not None and self._checkout_mode != batch:
                raise RuntimeError("abort before changing checkout mode")
            for endpoint in targets:
                if endpoint not in self._outputs:
                    continue
                value = timestamp.get(endpoint) if isinstance(timestamp, Mapping) else timestamp
                if batch and endpoint.spec.kind == "stream":
                    if not isinstance(value, (tuple, list)) or len(value) != endpoint.spec.batch_size:
                        raise ValueError("provide batch_size timestamps for each Stream Publisher")
                    tuple(index(t) for t in value)
                elif value is not None:
                    index(value)
                elif not any(e in targets or e in self._reservations for e in self._inputs):
                    raise ValueError("provide a timestamp for each Publisher without an input source")
            self._checkout_mode = batch
            self._checkout_running = True
        try:
            with ExitStack() as intents:
                for endpoint in targets:
                    if (endpoint in self._outputs and endpoint not in self._reservations
                            and isinstance(endpoint._policy, StatePolicy)):
                        intents.enter_context(endpoint._endpoint.write_request())
                pending = [e for e in targets if e not in self._reservations]
                first = True
                while pending:
                    self._check_wait(None if first else deadline, stop_event)
                    first = False
                    for reservation in self._reservations.values():
                        reservation.check()
                    for endpoint in tuple(pending):
                        value = timestamp.get(endpoint) if isinstance(timestamp, Mapping) else timestamp
                        if endpoint in self._outputs and value is None:
                            source_endpoint = next((e for e in self._inputs
                                                    if e in targets or e in self._reservations), None)
                            source = self._reservations.get(source_endpoint)
                            if source is None:
                                continue
                            value = source.timestamp[-1] if isinstance(source.timestamp, (tuple, list)) else source.timestamp
                        try:
                            reservation = endpoint._endpoint.reserve(value, batch=batch)
                        except BlockingIOError:
                            continue
                        self._reservations[endpoint] = reservation
                        pending.remove(endpoint)
                    if pending:
                        self._check_wait(deadline, stop_event)
                        delay = self.interval
                        if deadline is not None:
                            delay = min(delay, max(0, deadline - monotonic()))
                        self._stop.wait(delay)
                self._check_wait(None, stop_event)
                for endpoint in targets:
                    if endpoint not in self._frames:
                        self._frames[endpoint] = self._reservations[endpoint].checkout()
                with self._condition:
                    self._check_wait(None, stop_event)
                    self._request = None
                    self._refresh()
                return MappingProxyType({e: self._frames[e] for e in targets})
        finally:
            with self._condition:
                self._checkout_running = False
                self._refresh()

    def commit(self):
        """Seal checked-out outputs and hand them to send; inputs stay borrowed."""
        with self._condition:
            self._manual_check()
            self._check()
            if self._request is not None:
                raise RuntimeError("finish checkout before commit")
            outputs = [e for e in self._outputs if e in self._reservations]
            committed = []
            try:
                for endpoint in outputs:
                    self._reservations[endpoint].check()
                for endpoint in outputs:
                    reservation = self._reservations[endpoint]
                    reservation.commit()
                    committed.append(reservation)
                    del self._reservations[endpoint]
                    self._frames.pop(endpoint, None)
            except BaseException as exc:
                if committed:
                    self._queue.extend(r for r in committed if r.queued)
                    self._error = exc
                raise
            finally:
                self._refresh()
            self._enqueue([r for r in committed if r.queued])

    def _release(self, endpoints, *, consume):
        errors = []
        for endpoint in endpoints:
            reservation = self._reservations.get(endpoint)
            if reservation is None:
                continue
            try:
                reservation.release(consume=consume)
            except StreamRemovedError:
                pass
            except BaseException as exc:
                errors.append(exc)
                self._unreleased.append((reservation, consume))
                del self._reservations[endpoint]
                self._frames.pop(endpoint, None)
                continue
            del self._reservations[endpoint]
            self._frames.pop(endpoint, None)
        self._refresh()
        if errors:
            if self._error is None:
                self._error = errors[0]
            for error in errors[1:]:
                errors[0].add_note(f"Additional release failure: {error!r}")
            raise errors[0]

    def complete(self):
        """Mark checked-out inputs used and return them (consume FIFO input)."""
        with self._condition:
            self._manual_check()
            # Cleanup must remain possible after publication or stream failure.
            if self._request is not None:
                raise RuntimeError("finish checkout before complete")
            self._release(self._inputs, consume=True)

    def abort(self):
        """Cancel outstanding work; input remains unread, committed sends remain."""
        with self._condition:
            self._manual_check()
            self._request = None
            self._release(list(reversed(self._inputs + self._outputs)), consume=False)

    def _invoke(self, method, *args, **kwargs):
        self._calls.wrapper = True
        try:
            return getattr(self, method)(*args, **kwargs)
        finally:
            self._calls.wrapper = False

    def acquire(self, timestamp=None, *, timeout=None, stop_event=None):
        return self._acquire(timestamp=timestamp, timeout=timeout, stop_event=stop_event, batch=False)

    def acquire_batch(self, timestamps=None, *, timeout=None, stop_event=None):
        return self._acquire(timestamp=timestamps, timeout=timeout, stop_event=stop_event, batch=True)

    @contextmanager
    def _acquire(self, *, timestamp, timeout, stop_event, batch):
        """With-only convenience wrapper over the manual lifecycle API."""
        with self._condition:
            self._check()
            if self._with_active or self._busy:
                raise RuntimeError("Node already has active work")
            self._with_active = True
        entered = False
        try:
            self._invoke("reserve", timestamp=timestamp)
            frames = self._invoke("checkout_batch" if batch else "checkout", timeout=timeout, stop_event=stop_event)
            entered = True
            yield frames
            self._invoke("commit")
            self._invoke("complete")
        finally:
            primary = sys.exception()
            failures = []
            if entered and primary is not None:
                try:
                    self._invoke("complete")
                except BaseException as exc:
                    failures.append(exc)
            if primary is not None or self._busy:
                try:
                    self._invoke("abort")
                except BaseException as exc:
                    failures.append(exc)
            with self._condition:
                self._with_active = False
                self._condition.notify_all()
            if failures:
                if primary is not None:
                    for exc in failures:
                        primary.add_note(f"Reservation cleanup failed: {exc!r}")
                else:
                    raise failures[0]

    def _enqueue(self, reservations):
        if not reservations:
            return
        with self._condition:
            self._queue.extend(reservations)
            if self._worker is None:
                worker = Thread(target=self._send, name="sigbus-send", daemon=True)
                try:
                    worker.start()
                except Exception as exc:
                    self._error = exc
                else:
                    self._worker = worker
            self._condition.notify_all()

    def _send(self):
        while not self._send_stop.is_set():
            with self._condition:
                while not self._queue and not self._send_stop.is_set():
                    self._condition.wait()
                if self._send_stop.is_set() or self._error is not None:
                    return
                reservation = self._queue[0]
            try:
                reservation.send()
            except OSError as exc:
                if isinstance(exc, (PermissionError, FileNotFoundError, AckTimeoutError)):
                    with self._condition:
                        self._error = exc
                        self._condition.notify_all()
                    return
                # Payload and publication metadata remain pinned until success.
                self._send_stop.wait(self.interval)
            except Exception as exc:
                with self._condition:
                    self._error = exc
                    self._condition.notify_all()
                return
            else:
                with self._condition:
                    self._queue.popleft()
                    self._condition.notify_all()

    def _wait_sends(self, deadline, stop_event=None):
        with self._condition:
            while self._queue:
                self._check(closing=True)
                if self._cancelled(stop_event):
                    return False
                remaining = None if deadline is None else deadline - monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                delay = remaining
                if self.stop_event is not None or stop_event is not None:
                    delay = self.interval if delay is None else min(delay, self.interval)
                self._condition.wait(delay)
            self._check(closing=True)
            return True

    def drain(self, *, grace_period=0, timeout=None, stop_event=None):
        """Flush committed tails and return True when all sends finish.

        Grace expiry is normal: it triggers flushing. Timeout/cancellation returns
        False without discarding output. Fatal publication failures still raise.
        During grace another thread may fill batches; once flushing starts, new
        acquisitions are rejected until drain returns. Existing AP work is never
        committed by drain.
        """
        if not math.isfinite(grace_period) or grace_period < 0:
            raise ValueError("grace_period must be finite and nonnegative")
        deadline = _deadline(timeout)
        grace_deadline = monotonic() + grace_period
        with self._condition:
            self._check()
            if self._busy or self._draining:
                raise RuntimeError("leave the acquisition/drain before draining")
            self._draining = True
            try:
                while (monotonic() < grace_deadline and
                       (self._busy or any(p.has_pending() for p in self._outputs))):
                    self._check()
                    if self._cancelled(stop_event):
                        return False
                    if deadline is not None and monotonic() >= deadline:
                        break
                    delay = min(self.interval, grace_deadline - monotonic())
                    if deadline is not None:
                        delay = min(delay, max(0, deadline - monotonic()))
                    self._condition.wait(max(0, delay))
                self._flushing = True
                while self._busy:
                    self._check()
                    if self._cancelled(stop_event) or (deadline is not None and monotonic() >= deadline):
                        return False
                    self._condition.wait(self.interval)
                self._check()
                self._queue_tails()
                return self._wait_sends(deadline, stop_event)
            finally:
                self._draining = self._flushing = False
                self._condition.notify_all()

    def _queue_tails(self):
        for endpoint in self._outputs:
            reservation = endpoint._endpoint.queue_pending()
            if reservation is not None:
                self._enqueue([reservation])

    def flush(self, *, timeout=None, stop_event=None):
        """Flush committed output immediately and wait; False means unfinished."""
        return self.drain(timeout=timeout, stop_event=stop_event)

    def discard(self, *, timeout=None):
        """Explicit terminal operation: discard unsent output and close resources."""
        return self._close(drain=False, timeout=timeout)

    def close(self, *, timeout=None):
        """Drain committed output and close; False retains unfinished resources."""
        return self._close(drain=True, timeout=timeout)

    def _close(self, *, drain, timeout=None):
        """Cancel waits and drain (including tails), or explicitly discard output.

        Registered endpoint resources close with Node. An unfinished close returns
        False and retains resources; retry it or explicitly discard().
        """
        deadline = _deadline(timeout)
        with self._condition:
            if self._closed:
                return True
            if self._processing or self._draining:
                raise RuntimeError("leave the AP processing context before closing Node")
            self._closing = True
            self.stop()
            while self._checkout_running or self._with_active:
                remaining = None if deadline is None else deadline - monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(remaining)
            if self._busy:
                self.abort()
            while self._unreleased:
                reservation, consume = self._unreleased[0]
                try:
                    reservation.release(consume=consume)
                except StreamRemovedError:
                    pass
                self._unreleased.pop(0)
            if drain:
                self._check(closing=True)
                self._queue_tails()
        if drain and not self._wait_sends(deadline):
            return False
        self._send_stop.set()
        with self._condition:
            self._condition.notify_all()
        if self._worker is not None:
            self._worker.join(None if deadline is None else max(0, deadline - monotonic()))
            if self._worker.is_alive():
                return False
        if not drain:
            for endpoint in self._outputs:
                try:
                    endpoint._endpoint.discard_pending()
                except StreamRemovedError:
                    pass
            while self._queue:
                reservation = self._queue[0]
                try:
                    reservation.discard()
                except StreamRemovedError:
                    pass
                self._queue.popleft()
        with self._condition:
            for endpoint in self._inputs + self._outputs:
                endpoint._resources.close(self._resource_owner)
            self._closed = True
            return True

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            if not self.close():
                raise InterruptedError("Node close cancelled; committed output is retained")
        else:
            try:
                self.discard()
            except BaseException as cleanup_error:
                exc.add_note(f"Node cleanup failed: {cleanup_error!r}")
        return False


class Node(_Lifecycle):
    """A bus participant owning a group of Publisher/Subscriber resources."""
