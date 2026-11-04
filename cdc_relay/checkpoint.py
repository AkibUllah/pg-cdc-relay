"""The last delivered position, kept next to the relay.

The replication slot is the authoritative checkpoint: the server resumes streaming from the
slot's ``confirmed_flush_lsn``. But feedback is sent after delivery, so a crash between the
two makes the server resend transactions the sink already has. The local checkpoint is written
right after each delivered batch, and transactions ending at or before it are skipped on
restart, which removes that window for everything except a crash during ``send`` itself.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Protocol

from cdc_relay.lsn import format_lsn, parse_lsn


class Checkpoint(Protocol):
    def load(self) -> int: ...

    def save(self, lsn: int) -> None: ...


class MemoryCheckpoint:
    def __init__(self, lsn: int = 0) -> None:
        self.lsn = lsn
        self.saves = 0

    def load(self) -> int:
        return self.lsn

    def save(self, lsn: int) -> None:
        self.lsn = lsn
        self.saves += 1


class CheckpointError(RuntimeError):
    pass


class FileCheckpoint:
    def __init__(self, path: str, slot: str) -> None:
        self.path = path
        self.slot = slot

    def load(self) -> int:
        try:
            with open(self.path, encoding="utf-8") as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return 0
        except (OSError, ValueError) as exc:
            raise CheckpointError(f"unreadable checkpoint {self.path}: {exc}") from exc
        if data.get("slot") != self.slot:
            raise CheckpointError(
                f"checkpoint {self.path} belongs to slot {data.get('slot')!r}, not {self.slot!r}; "
                "use a separate checkpoint file per slot"
            )
        return parse_lsn(data["lsn"])

    def save(self, lsn: int) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        payload = {
            "slot": self.slot,
            "lsn": format_lsn(lsn),
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".checkpoint-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
