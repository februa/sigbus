from hashlib import sha256
from pathlib import Path
import tempfile

from .._persistent import PersistentTransport


class SharedMemoryTransport(PersistentTransport):
    kind = "shared_memory"

    def __init__(self, namespace="default", slots=3, *, directory=None):
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("namespace must be a nonempty string")
        root = Path(directory) if directory is not None else Path(tempfile.gettempdir()) / "sigbus-shm"
        super().__init__(root / sha256(namespace.encode()).hexdigest(), slots)



SharedMemoryAdapter = SharedMemoryTransport
