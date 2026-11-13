from hashlib import sha256
import re

from .._persistent import PersistentTransport


class FileTransport(PersistentTransport):
    kind = "file"

    def _stream_directory(self, name):
        if not isinstance(name, str) or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", name) is None:
            raise ValueError("FileTransport stream name must be 1-63 lowercase ASCII "
                             "letters, digits, underscores or hyphens, starting with a letter or digit")
        directory = self.directory / name
        legacy = self.directory / sha256(name.encode()).hexdigest()
        if legacy.exists():
            raise ValueError(f"Stream {name!r}: legacy hashed directory exists at {legacy}; "
                             "stop all users and rename it to the stream name, "
                             "or use a new storage directory")
        return directory


FileAdapter = FileTransport
