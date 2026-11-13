from threading import Event, Thread


class _Endpoint:
    """Common control worker for every transport; never completes a data lease."""
    def __init__(self, storage, endpoint_id, *, ack=None):
        self._storage = storage
        self._endpoint_id = endpoint_id
        self._stop = Event()
        self._worker = None
        self._control_error = None
        if ack is not None:
            self._worker = Thread(target=self._respond, args=(ack.interval,),
                                  name="sigbus-ack", daemon=True)
            try:
                self._worker.start()
            except BaseException:
                storage.detach(endpoint_id)
                raise

    def _respond(self, interval):
        while not self._stop.wait(interval):
            try:
                self._storage.ack(self._endpoint_id)
            except Exception as exc:
                self._control_error = exc
                return

    def _check(self):
        if self._control_error is not None:
            raise self._control_error

    def ack(self):
        self._check()
        self._storage.ack(self._endpoint_id)

    def close(self):
        # A rejected close during a lease must leave the ACK worker running.
        self._storage.detach(self._endpoint_id)
        self._stop.set()
        if self._worker is not None:
            self._worker.join()


class InProcessWriteEndpoint(_Endpoint):
    def write_request(self):
        self._check()
        return self._storage.write_request(self._endpoint_id)

    def acquire(self, timestamp):
        self._check()
        return self._storage.acquire_write(self._endpoint_id, timestamp)

    def has_pending(self):
        self._check()
        return self._storage.has_pending(self._endpoint_id)

    def flush(self):
        self._check()
        self._storage.flush_pending(self._endpoint_id)


class InProcessReadEndpoint(_Endpoint):
    def has_frame(self):
        self._check()
        return self._storage.has_frame(self._endpoint_id)

    def acquire_batch(self):
        self._check()
        return self._storage.acquire_read(self._endpoint_id, batch=True)

    def acquire(self):
        self._check()
        return self._storage.acquire_read(self._endpoint_id)
