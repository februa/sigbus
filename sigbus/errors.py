"""Acquisition failures remain compatible with BlockingIOError handlers."""


class NoFrameAvailable(BlockingIOError):
    """This subscriber currently has no unread frame."""


class NoWritableSlot(BlockingIOError):
    """No slot can be safely reserved under the configured policy."""


class StreamInUseError(RuntimeError):
    """Removal refused because endpoints remain registered."""


class StreamRemovedError(RuntimeError):
    """The stream attached to this endpoint was removed or replaced."""


class StreamCorruptedError(RuntimeError):
    """Stream metadata or payload structure is invalid."""


class StreamPermissionError(PermissionError):
    """The requested stream operation lacks filesystem or OS permission."""


class ParticipantExcludedError(RuntimeError):
    """This endpoint expired; close it and create a new endpoint to rejoin."""


class AckTimeoutError(TimeoutError):
    """A participant expired with on_timeout='raise'; its obligations remain."""
