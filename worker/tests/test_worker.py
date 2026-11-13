import threading

import pytest
from worker import Event, ModuleSpec, Periodic, RunResult, Stage, State, Worker, WorkerError
from worker.adapters.sigbus import StreamReady


class Module:
    def __init__(self, name="module", log=None, count=1):
        self.name = name
        self.log = [] if log is None else log
        self.remaining = count
        self.threads = []

    def initialize(self):
        self.threads.append(threading.get_ident())
        self.log.append((self.name, "initialize"))

    def run_once(self):
        self.threads.append(threading.get_ident())
        self.log.append((self.name, "run"))
        self.remaining -= 1
        return RunResult.FINISHED if self.remaining == 0 else RunResult.PROCESSED

    def finalize(self):
        self.threads.append(threading.get_ident())
        self.log.append((self.name, "finalize"))


def always(stage):
    return lambda: True


def stage(name, module, schedule=None):
    return Stage(name, [ModuleSpec("module", module)], schedule or Periodic(1000))


def test_standalone():
    module = Module()
    module.initialize()
    assert module.run_once() is RunResult.FINISHED
    module.finalize()
    assert len(module.log) == 3


def test_stage_order_and_thread_affinity():
    log = []
    a, b = Module("a", log, 3), Module("b", log, 3)
    worker = Worker([Stage("ab", [ModuleSpec("a", a), ModuleSpec("b", b)], Periodic(1000))])
    reports = worker.run()
    assert log[:2] == [("a", "initialize"), ("b", "initialize")]
    assert [name for name, action in log if action == "run"] == ["a", "b"] * 3
    assert log[-2:] == [("b", "finalize"), ("a", "finalize")]
    assert len(set(a.threads + b.threads)) == 1
    assert a.threads[0] != threading.get_ident()
    assert reports["ab"].stats.executions == 3
    assert all(r.stats.executions == 3 for r in reports["ab"].modules.values())
    with pytest.raises(RuntimeError, match="single-use"):
        worker.run()


def test_stage_threads_overlap():
    # Serial execution would time out: both calls must enter before either exits.
    barrier = threading.Barrier(2, timeout=2)
    class Concurrent(Module):
        def run_once(self):
            barrier.wait()
            return super().run_once()
    a, b = Concurrent(), Concurrent()
    Worker([stage("a", a), stage("b", b, Event(always))]).run()
    assert a.threads[0] != b.threads[0]


def test_pipeline_processes_next_item_while_downstream_is_busy():
    published, downstream_started, second_published = (threading.Event() for _ in range(3))
    class Source(Module):
        def run_once(self):
            if not published.is_set():
                published.set()
                return RunResult.PROCESSED
            assert downstream_started.wait(2)
            second_published.set()
            return RunResult.FINISHED
    class Sink(Module):
        def run_once(self):
            downstream_started.set()
            assert second_published.wait(2)
            return RunResult.FINISHED
    Worker([stage("source", Source()),
            stage("sink", Sink(), Event(lambda s: published.is_set))]).run()


def test_idle_aborts_pass_without_rollback():
    log = []
    class Idle(Module):
        def run_once(self):
            self.log.append((self.name, "run"))
            worker.stop()
            return RunResult.IDLE
    worker = Worker([Stage("chain", [ModuleSpec("a", Module("a", log, 10)),
        ModuleSpec("b", Idle("b", log)), ModuleSpec("c", Module("c", log))], Periodic(10))])
    report = worker.run()["chain"]
    assert [name for name, action in log if action == "run"] == ["a", "b"]
    assert report.stats.idle == 1
    assert report.modules["a"].stats.processed == 1
    assert report.modules["c"].stats.executions == 0


def test_finished_allows_final_downstream_pass_then_stops_stage():
    a, b = Module(count=1), Module(count=10)
    report = Worker([Stage("chain", [ModuleSpec("a", a), ModuleSpec("b", b)], Periodic(10))]).run()["chain"]
    assert report.stats.executions == 1
    assert report.modules["b"].stats.processed == 1


def test_periodic_absolute_schedule_skips_without_drift():
    condition = Periodic(10).bind(None, 0)
    assert condition.ready(0)
    assert condition.after_run(0.25) == 2
    assert condition.next_check() == pytest.approx(0.3)
    assert not condition.ready(0.29)
    assert condition.ready(0.31)
    assert condition.after_run(0.32) == 0
    assert condition.next_check() == pytest.approx(0.4)


def test_event_throttles_idle_and_condition_checks():
    calls = []
    condition = Event(lambda s: lambda: calls.append(1) or True, interval=0.1).bind(None, 0)
    assert condition.ready(0)
    condition.after_run(0.01)
    assert not condition.ready(0.05)
    assert condition.ready(0.12)
    assert len(calls) == 2


def test_failure_cleanup_including_partial_initialize():
    log = []
    class Broken(Module):
        def initialize(self):
            super().initialize()
            raise ValueError("init failed")
        def finalize(self):
            super().finalize()
            raise ValueError("cleanup failed")
    worker = Worker([Stage("chain", [ModuleSpec("a", Module("a", log)),
        ModuleSpec("b", Broken("b", log)), ModuleSpec("c", Module("c", log))], Periodic(1))])
    with pytest.raises(WorkerError) as error:
        worker.run()
    assert str(error.value.__cause__) == "init failed"
    assert log[-2:] == [("b", "finalize"), ("a", "finalize")]
    assert len(worker.reports["chain"].modules["b"].errors) == 2


def test_error_stops_other_waiting_stage():
    class Broken(Module):
        def run_once(self):
            raise ValueError("run failed")
    worker = Worker([stage("bad", Broken()),
        stage("peer", Module(), Event(lambda s: lambda: False, interval=60))])
    with pytest.raises(WorkerError):
        worker.run()
    assert worker.state is State.FAILED
    assert worker.reports["peer"].stats.executions == 0
    assert worker.reports["peer"].state is State.STOPPED


def test_all_initialized_before_any_run_and_cleanup_after_all_runs():
    a_initialized, b_initialized, b_done = (threading.Event() for _ in range(3))
    class A(Module):
        def initialize(self):
            a_initialized.set()
        def run_once(self):
            assert b_initialized.is_set()
            return RunResult.FINISHED
        def finalize(self):
            assert b_done.is_set()
    class B(Module):
        def initialize(self):
            b_initialized.set()
        def run_once(self):
            assert a_initialized.is_set()
            b_done.set()
            return RunResult.FINISHED
    Worker([stage("a", A()), stage("b", B())]).run()


def test_stop_interrupts_long_wait():
    checked = threading.Event()
    def predicate():
        checked.set()
        return False
    worker = Worker([stage("waiting", Module(), Event(lambda s: predicate, interval=60))])
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        assert checked.wait(2)
    finally:
        worker.stop()
        thread.join(2)
    assert not thread.is_alive()
    assert worker.state is State.STOPPED


def test_notification_condition_extension():
    checked, available = threading.Event(), threading.Event()
    class Notification:
        def bind(self, stage, start):
            return self
        def ready(self, now):
            checked.set()
            return available.is_set()
        def next_check(self):
            return float("inf")
        def after_run(self, now):
            return 0
    worker = Worker([stage("notify", Module(), Notification())])
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        assert checked.wait(2)
        available.set()
        worker.wake("notify")
        thread.join(2)
        assert not thread.is_alive()
        assert worker.reports["notify"].stats.executions == 1
    finally:
        worker.stop()
        thread.join(2)


def test_two_subscribers_receive_same_stream():
    from sigbus import Bus, StreamSpec, InProcessTransport
    transport = InProcessTransport()
    bus = Bus(transport)
    spec = StreamSpec("worker-test", (), "i8", slots=4)
    pub = bus.publisher(spec)
    values = []
    class Reader:
        def initialize(self):
            self.sub = bus.subscriber(spec)
        def run_once(self):
            with self.sub.acquire() as frame:
                values.append(frame.payload.item())
            return RunResult.FINISHED
        def finalize(self):
            self.sub.close()
    try:
        with pub.acquire(timestamp=0) as frame:
            frame.payload[...] = 42
        Worker([stage(name, Reader(), Event(lambda s: StreamReady(s.module("module").sub)))
                for name in ("a", "b")]).run()
        assert values == [42, 42]
    finally:
        pub.close()
        transport.close()


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_invalid_schedule(value):
    with pytest.raises(ValueError):
        Periodic(value)
    with pytest.raises(ValueError):
        Event(always, interval=value)


def test_no_shared_module_registration():
    module = Module()
    with pytest.raises(ValueError, match="distinct"):
        Worker([stage("a", module), stage("b", module)])


def test_stage_base_exception_propagates_after_cleanup():
    class Interrupted(Module):
        def run_once(self):
            raise KeyboardInterrupt()
    module = Interrupted()
    worker = Worker([stage("interrupt", module)])
    with pytest.raises(KeyboardInterrupt):
        worker.run()
    assert module.log[-1][1] == "finalize"


def test_stage_budget_includes_all_module_processing():
    import time
    class Slow(Module):
        def run_once(self):
            time.sleep(0.005)
            return super().run_once()
    worker = Worker([Stage("combined", [ModuleSpec("a", Slow(count=2)),
        ModuleSpec("b", Slow(count=2))], Periodic(1000))])
    report = worker.run()["combined"]
    assert report.stats.skipped >= 5
    assert report.stats.processing_seconds >= sum(
        module.stats.processing_seconds for module in report.modules.values())


def test_condition_failure_cleans_up_initialized_modules():
    def broken(stage):
        raise ValueError("condition failed")
    module = Module()
    worker = Worker([stage("broken", module, Event(broken))])
    with pytest.raises(WorkerError):
        worker.run()
    assert module.log == [("module", "initialize"), ("module", "finalize")]
    assert worker.reports["broken"].errors
    assert not worker.reports["broken"].modules["module"].errors


def test_stop_before_run_does_not_initialize_modules():
    module = Module()
    worker = Worker([stage("stopped", module)])
    worker.stop()
    worker.run()
    assert not module.log
    assert worker.state is State.STOPPED
