"""Stage threads in one process; modules within each stage run sequentially."""
from dataclasses import dataclass, field
from enum import Enum
import math
import threading
import time
import traceback
from typing import Protocol

from .scheduling import Schedule


class Module(Protocol):
    def initialize(self) -> None: ...
    def run_once(self) -> "RunResult": ...
    def finalize(self) -> None: ...


class RunResult(Enum):
    PROCESSED = "processed"
    IDLE = "idle"
    FINISHED = "finished"


class State(Enum):
    NEW = "new"
    STARTING = "starting"
    READY = "ready"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True)
class ModuleSpec:
    name: str
    module: Module

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("name must be a nonempty string")
        for method in ("initialize", "run_once", "finalize"):
            if not callable(getattr(self.module, method, None)):
                raise TypeError(f"module must provide {method}()")


@dataclass(frozen=True)
class Stage:
    name: str
    modules: tuple[ModuleSpec, ...]
    schedule: Schedule

    def __post_init__(self):
        object.__setattr__(self, "modules", tuple(self.modules))
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("stage name must be a nonempty string")
        names = [spec.name for spec in self.modules]
        if not names or len(names) != len(set(names)):
            raise ValueError("stage requires modules with unique names")
        if not callable(getattr(self.schedule, "bind", None)):
            raise TypeError("schedule must provide bind(stage, start)")

    def module(self, name: str) -> Module:
        """Access a module when assembling an application-defined condition."""
        for spec in self.modules:
            if spec.name == name:
                return spec.module
        raise KeyError(name)


@dataclass
class Stats:
    executions: int = 0
    processed: int = 0
    idle: int = 0
    skipped: int = 0
    processing_seconds: float = 0.0
    max_processing_seconds: float = 0.0


@dataclass
class Report:
    state: State = State.NEW
    stats: Stats = field(default_factory=Stats)
    errors: list[str] = field(default_factory=list)


@dataclass
class StageReport(Report):
    modules: dict[str, Report] = field(default_factory=dict)


class WorkerError(RuntimeError):
    pass


class Worker:
    """Single-use coordinator. Each Stage owns one non-daemon thread.

    All lifecycle methods of a module run on its stage thread. Stop is
    cooperative; all hooks must return in finite time. No process is created.
    """
    def __init__(self, stages):
        self.stages = tuple(stages)
        names = [stage.name for stage in self.stages]
        if not names or len(names) != len(set(names)):
            raise ValueError("provide stages with unique names")
        modules = [spec.module for stage in self.stages for spec in stage.modules]
        if len({id(module) for module in modules}) != len(modules):
            raise ValueError("each registration must own a distinct module instance")
        self.reports = {stage.name: StageReport(modules={spec.name: Report() for spec in stage.modules})
                        for stage in self.stages}
        self.state = State.NEW
        self._stop = threading.Event()
        self._wakes = {stage.name: threading.Event() for stage in self.stages}
        self._run_lock = threading.Lock()
        self._error_lock = threading.Lock()
        self._failures = []

    def stop(self):
        """Stop future module calls and interrupt schedule waits, not active code."""
        self._stop.set()
        self.wake()

    def wake(self, stage=None):
        """Notify one stage, or all stages; condition state must already be updated."""
        if stage is None:
            for event in self._wakes.values():
                event.set()
        else:
            self._wakes[stage].set()

    def _error(self, stage, module, exc):
        report = self.reports[stage.name]
        detail = traceback.format_exc()
        report.errors.append(detail)
        if module is not None:
            report.modules[module.name].errors.append(detail)
        with self._error_lock:
            self._failures.append(exc)
        self.stop()

    @staticmethod
    def _timed_call(callback, stats):
        stats.executions += 1
        began = time.monotonic()
        try:
            return callback()
        finally:
            elapsed = time.monotonic() - began
            stats.processing_seconds += elapsed
            stats.max_processing_seconds = max(stats.max_processing_seconds, elapsed)

    def _stage_thread(self, stage, ready, done, gate, cleanup, epoch):
        report = self.reports[stage.name]
        initialized = []
        current = None
        try:
            report.state = State.STARTING
            for spec in stage.modules:
                if self._stop.is_set():
                    break
                current = spec
                initialized.append(spec)
                report.modules[spec.name].state = State.STARTING
                spec.module.initialize()
                report.modules[spec.name].state = State.READY
            current = None
            report.state = State.READY
            ready.set()
            gate.wait()
            if self._stop.is_set():
                return
            condition = stage.schedule.bind(stage, epoch[0])
            report.state = State.RUNNING
            wake = self._wakes[stage.name]
            while not self._stop.is_set():
                wake.clear()
                if condition.ready(time.monotonic()):
                    if self._stop.is_set():
                        break
                    # FINISHED terminates the whole stage after this pass. Remaining
                    # modules still run once to consume the final upstream output.
                    finish = False
                    idle = False
                    def run_pass():
                        nonlocal current, finish, idle
                        for spec in stage.modules:
                            if self._stop.is_set():
                                break
                            current = spec
                            module_report = report.modules[spec.name]
                            module_report.state = State.RUNNING
                            result = self._timed_call(spec.module.run_once, module_report.stats)
                            if not isinstance(result, RunResult):
                                raise TypeError("run_once() must return RunResult")
                            if result is RunResult.PROCESSED:
                                module_report.stats.processed += 1
                            elif result is RunResult.IDLE:
                                module_report.stats.idle += 1
                                idle = True
                                break
                            else:
                                finish = True
                        current = None
                    self._timed_call(run_pass, report.stats)
                    if idle:
                        report.stats.idle += 1
                    elif not self._stop.is_set():
                        report.stats.processed += 1
                    if finish:
                        break
                    report.stats.skipped += condition.after_run(time.monotonic())
                if not self._stop.is_set():
                    deadline = condition.next_check()
                    if math.isnan(deadline) or deadline == -math.inf:
                        raise ValueError("next_check must be a monotonic time or positive infinity")
                    delay = max(0, deadline - time.monotonic())
                    wake.wait(None if delay == math.inf else delay)
        except BaseException as exc:
            self._error(stage, current, exc)
        finally:
            report.state = State.STOPPING
            ready.set()  # release startup coordinator even when initialize failed
            done.set()
            # Keep endpoints alive until every stage has left its execution loop.
            cleanup.wait()
            for spec in reversed(initialized):
                module_report = report.modules[spec.name]
                module_report.state = State.STOPPING
                try:
                    spec.module.finalize()
                except BaseException as exc:
                    self._error(stage, spec, exc)
                module_report.state = State.FAILED if module_report.errors else State.STOPPED
            for module_report in report.modules.values():
                if module_report.state is State.NEW:
                    module_report.state = State.STOPPED
            report.state = State.FAILED if report.errors else State.STOPPED

    def run(self):
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("Worker is already running")
        try:
            if self.state is not State.NEW:
                raise RuntimeError("Worker instances are single-use")
            return self._run()
        finally:
            self._run_lock.release()

    def _run(self):
        self.state = State.STARTING
        gate, cleanup = threading.Event(), threading.Event()
        epoch = [0.0]
        started = []
        coordinator_error = None
        try:
            for stage in self.stages:
                ready, done = threading.Event(), threading.Event()
                thread = threading.Thread(target=self._stage_thread,
                    args=(stage, ready, done, gate, cleanup, epoch),
                    name=f"worker:{stage.name}", daemon=False)
                thread.start()
                started.append((thread, ready, done))
            while not all(ready.is_set() for _, ready, _ in started):
                if self._stop.wait(0.01):
                    break
            epoch[0] = time.monotonic()
            self.state = State.RUNNING
            gate.set()
            for _, _, done in started:
                while not done.wait(0.05):
                    pass
        except BaseException as exc:
            coordinator_error = exc
            self.stop()
        finally:
            self.state = State.STOPPING
            gate.set()
            # Cooperatively wait before releasing resources used by other stages.
            for _, _, done in started:
                done.wait()
            cleanup.set()
            for thread, _, _ in started:
                thread.join()
            for report in self.reports.values():
                if report.state is State.NEW:
                    report.state = State.STOPPED
                    for module_report in report.modules.values():
                        module_report.state = State.STOPPED
            self.state = State.FAILED if coordinator_error or self._failures else State.STOPPED
        if coordinator_error is not None:
            raise coordinator_error
        if self._failures:
            # Preserve interrupt/system-exit semantics across the thread boundary.
            interrupt = next((exc for exc in self._failures if not isinstance(exc, Exception)), None)
            if interrupt is not None:
                raise interrupt
            failed = ", ".join(name for name, report in self.reports.items() if report.errors)
            raise WorkerError(f"Stage failure: {failed}; see reports for tracebacks") from self._failures[0]
        return self.reports
