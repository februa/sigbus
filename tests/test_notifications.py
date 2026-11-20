import threading
import math
from sigbus.worker import Event, Periodic, Worker, Stage, ModuleSpec, RunResult


class Receiver:
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.calls = 0
    def initialize(self):
        pass
    def run_once(self):
        self.calls += 1
        if self.calls == 1:
            self.started.set()
            assert self.release.wait(2)
            return RunResult.PROCESSED
        return RunResult.FINISHED
    def finalize(self):
        pass


def test_event_has_no_polling_deadline():
    condition = Event().bind(None, 0)
    assert not condition.ready(0)
    assert condition.next_check() == math.inf
    condition.notification.set()
    assert condition.ready(1)
    assert not condition.ready(2)
    assert condition.after_run(2) == 0
    assert condition.next_check() == math.inf


def test_notification_before_start_and_during_execution():
    receiver = Receiver()
    worker = Worker([Stage('receive', [ModuleSpec('receive', receiver)], Event())])
    worker.wake('receive')  # retained before bind/initialize
    errors = []
    def run():
        try:
            worker.run()
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert receiver.started.wait(2)
        worker.wake('receive')
        worker.wake('receive')  # coalesced while processing
        receiver.release.set()
        thread.join(2)
        assert not thread.is_alive()
        assert receiver.calls == 2
        assert not errors
    finally:
        receiver.release.set()
        worker.stop()
        thread.join(2)


def test_stop_interrupts_unnotified_event():
    receiver = Receiver()
    worker = Worker([Stage('receive', [ModuleSpec('receive', receiver)], Event())])
    thread = threading.Thread(target=worker.run)
    thread.start()
    worker.stop()
    thread.join(2)
    assert not thread.is_alive()
    assert receiver.calls == 0
