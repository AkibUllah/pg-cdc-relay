"""The last delivered position, kept next to the relay.

The replication slot is the authoritative checkpoint: the server resumes streaming from the
slot's ``confirmed_flush_lsn``. But feedback is sent after delivery, so a crash between the
two makes the server resend transactions the sink already has. The local checkpoint is written
right after each delivered batch, and transactions ending at or before it are skipped on
restart, which removes that window for everything except a crash during ``send`` itself.

It also records whether the initial snapshot finished. The exported snapshot dies with the
connection that created the slot, so a snapshot cut short by a crash cannot be resumed: a
``pending`` state tells the next start to drop the slot and take the snapshot again.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Protocol

from cdc_relay.lsn import format_lsn, parse_lsn

SNAPSHOT_PENDING, SNAPSHOT_DONE = "pending", "done"


class Checkpoint(Protocol):
    def load(self) -> int: ...

    def save(self, lsn: int) -> None: ...

    def snapshot_state(self) -> str | None: ...

    def save_snapshot(self, state: str, lsn: int = 0) -> None: ...


class MemoryCheckpoint:
    def __init__(self, lsn: int = 0, snapshot: str | None = None) -> None:
        self.lsn = lsn
        self.snapshot = snapshot
        self.saves = 0

    def load(self) -> int:
        return self.lsn

    def save(self, lsn: int) -> None:
        self.lsn = lsn
        self.saves += 1

    def snapshot_state(self) -> str | None:
        return self.snapshot

    def save_snapshot(self, state: str, lsn: int = 0) -> None:
        self.snapshot = state
        self.lsn = lsn


class CheckpointError(RuntimeError):
    pass


class FileCheckpoint:
    def __init__(self, path: str, slot: str) -> None:
        self.path = path
        self.slot = slot
        # Snapshot state as last read or written, so saving a position does not reread the file.
        self._snapshot: str | None = None
        self._known = False

    def _read(self) -> dict | None:
        try:
            with open(self.path, encoding="utf-8") as fh:
                data = json.load(fh)
        except FileNotFoundError:
            self._snapshot, self._known = None, True
            return None
        except (OSError, ValueError) as exc:
            raise CheckpointError(f"unreadable checkpoint {self.path}: {exc}") from exc
        if not isinstance(data, dict) or data.get("slot") != self.slot:
            found = data.get("slot") if isinstance(data, dict) else None
            raise CheckpointError(
                f"checkpoint {self.path} belongs to slot {found!r}, not {self.slot!r}; "
                "use a separate checkpoint file per slot"
            )
        self._snapshot, self._known = data.get("snapshot"), True
        return data

    def load(self) -> int:
        data = self._read()
        return parse_lsn(data["lsn"]) if data else 0

    def save(self, lsn: int) -> None:
        if not self._known:
            self._read()
        self._write(lsn, self._snapshot)

    def snapshot_state(self) -> str | None:
        self._read()
        return self._snapshot

    def save_snapshot(self, state: str, lsn: int = 0) -> None:
        """Record the snapshot state; a new slot also starts from a new position."""
        self._write(lsn, state)

    def _write(self, lsn: int, snapshot: str | None) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        payload = {
            "slot": self.slot,
            "lsn": format_lsn(lsn),
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        if snapshot is not None:
            payload["snapshot"] = snapshot
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".checkpoint-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
            self._snapshot, self._known = snapshot, True
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
