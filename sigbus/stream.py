from dataclasses import dataclass, field
import math
from operator import index

import numpy as np


def _validate_layout(name, shape, dtype, layout, slots):
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a nonempty string")
    if slots is not None:
        if index(slots) < 1:
            raise ValueError("slots must be a positive integer")
        slots = index(slots)
    shape = tuple(index(n) for n in shape)
    dtype = np.dtype(dtype)
    if any(n <= 0 for n in shape):
        raise ValueError("shape dimensions must be positive")
    if dtype.hasobject or dtype.itemsize == 0 or dtype.subdtype is not None:
        raise ValueError("dtype must be fixed-size, non-object, without a subarray root")
    if layout != "row_major":
        raise ValueError("v1 supports row_major only")
    return shape, dtype, slots


@dataclass(frozen=True, slots=True)
class StreamSpec:
    name: str
    shape: tuple[int, ...]
    dtype: np.dtype
    rate_hz: float = field(kw_only=True)
    layout: str = "row_major"
    batch_size: int = 1
    slots: int | None = None
    kind = "stream"

    def __post_init__(self):
        if isinstance(self.rate_hz, bool) or not math.isfinite(self.rate_hz) or self.rate_hz <= 0:
            raise ValueError("rate_hz must be finite and positive (Frames/second)")
        object.__setattr__(self, "rate_hz", float(self.rate_hz))
        shape, dtype, slots = _validate_layout(self.name, self.shape, self.dtype, self.layout, self.slots)
        object.__setattr__(self, "slots", slots)
        if index(self.batch_size) < 1:
            raise ValueError("batch_size must be a positive integer")
        object.__setattr__(self, "batch_size", index(self.batch_size))
        object.__setattr__(self, "shape", shape)
        object.__setattr__(self, "dtype", dtype)


@dataclass(frozen=True, slots=True)
class StateSpec:
    """A repeatedly readable current value, without batching or consumption."""
    name: str
    shape: tuple[int, ...]
    dtype: np.dtype
    layout: str = "row_major"
    slots: int = 2
    kind = "state"

    @property
    def batch_size(self):
        # Internal payload layout is shared with stream storage.
        return 1

    def __post_init__(self):
        if self.slots is None:
            raise ValueError("StateSpec requires a positive slot count")
        shape, dtype, slots = _validate_layout(self.name, self.shape, self.dtype, self.layout, self.slots)
        object.__setattr__(self, "shape", shape)
        object.__setattr__(self, "dtype", dtype)
        object.__setattr__(self, "slots", slots)


@dataclass(slots=True)
class Frame:
    timestamp: int
    payload: np.ndarray
    generation: int | None = None


@dataclass(slots=True)
class Batch:
    """One slot's unread frames. Payload views are valid inside the lease only."""
    timestamps: tuple[int, ...]
    payload: np.ndarray

    def concatenate(self, axis=0) -> np.ndarray:
        """Join along a Frame axis; normalized axis 0 is zero-copy, others copy."""
        ndim = self.payload.ndim - 1
        axis = index(axis)
        if not -ndim <= axis < ndim:
            raise ValueError(f"axis {axis} is invalid for a Frame with {ndim} dimensions")
        axis %= ndim
        if axis == 0:
            shape = self.payload.shape
            result = self.payload.reshape((shape[0] * shape[1], *shape[2:]))
        else:
            result = np.concatenate(tuple(self.payload), axis=axis)
        result.flags.writeable = False
        return result
