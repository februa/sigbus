"""Nonblocking execution conditions. Times use the Worker's monotonic clock."""
from dataclasses import dataclass
import math
from typing import Callable, Protocol


class Condition(Protocol):
    def ready(self, now: float) -> bool: ...
    def next_check(self) -> float:
        """Earliest next check time, or infinity to wait for an explicit wake."""
        ...
    def after_run(self, finished_at: float) -> int:
        """Update scheduling state and return the number of skipped starts."""
        ...


class Schedule(Protocol):
    def bind(self, stage: object, start: float) -> Condition:
        """Create a fresh condition after all modules have initialized."""
        ...


def _positive(value, name):
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


@dataclass(frozen=True)
class Periodic:
    rate_hz: float

    def __post_init__(self):
        _positive(self.rate_hz, "rate_hz")
        _positive(1 / self.rate_hz, "period")

    def bind(self, stage, start):
        return _PeriodicCondition(start, 1 / self.rate_hz)


@dataclass
class _PeriodicCondition:
    deadline: float
    period: float

    def ready(self, now):
        return now >= self.deadline

    def next_check(self):
        return self.deadline

    def after_run(self, finished_at):
        self.deadline += self.period
        skipped = max(0, math.ceil((finished_at - self.deadline) / self.period))
        self.deadline += skipped * self.period
        return skipped


@dataclass(frozen=True)
class Event:
    """Poll a non-consuming predicate created by condition(stage).

    Check again after interval, including after a run, to bound empty spinning.
    The factory runs after initialize(), so it may access module-owned endpoints.
    """
    condition: Callable
    interval: float = 0.001

    def __post_init__(self):
        if not callable(self.condition):
            raise TypeError("condition must be a factory(stage) returning a predicate")
        _positive(self.interval, "interval")

    def bind(self, stage, start):
        predicate = self.condition(stage)
        if not callable(predicate):
            raise TypeError("condition factory must return a callable predicate")
        return _EventCondition(predicate, self.interval, start)


@dataclass
class _EventCondition:
    predicate: Callable
    interval: float
    deadline: float

    def ready(self, now):
        if now < self.deadline:
            return False
        self.deadline = now + self.interval
        return bool(self.predicate())

    def next_check(self):
        return self.deadline

    def after_run(self, finished_at):
        self.deadline = finished_at + self.interval
        return 0
