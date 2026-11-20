"""Ordered modules sharing one schedule and execution thread."""
from dataclasses import dataclass

from .scheduling import Schedule


@dataclass(frozen=True)
class ModuleSpec:
    name: str
    module: object

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

    def module(self, name: str) -> object:
        """Access a module when assembling an application-defined condition."""
        for spec in self.modules:
            if spec.name == name:
                return spec.module
        raise KeyError(name)


