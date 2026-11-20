"""Lifecycle state and runtime statistics."""
from dataclasses import dataclass, field
from enum import Enum


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


