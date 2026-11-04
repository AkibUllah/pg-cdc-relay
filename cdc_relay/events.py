"""Change events and transactions, the relay's view of the decoded stream."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from cdc_relay.lsn import format_lsn

INSERT, UPDATE, DELETE, TRUNCATE, MESSAGE = "insert", "update", "delete", "truncate", "message"


@dataclass
class ChangeEvent:
    op: str
    commit_lsn: int
    seq: int
    xid: int | None = None
    commit_ts: datetime | None = None
    schema: str | None = None
    table: str | None = None
    key: dict[str, Any] | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    # Columns of ``after`` that the server left out because their TOASTed value did not change.
    unchanged_toast: list[str] = field(default_factory=list)
    origin: str | None = None
    # Truncate: every table the statement truncated.
    tables: list[str] = field(default_factory=list)
    cascade: bool = False
    restart_identity: bool = False
    # Logical messages (pg_logical_emit_message).
    prefix: str | None = None
    content: bytes | None = None
    transactional: bool = True

    @property
    def id(self) -> str:
        """Stable across redelivery: the commit LSN is unique per transaction in a database."""
        return f"{format_lsn(self.commit_lsn)}:{self.seq}"

    @property
    def qualified_table(self) -> str | None:
        return f"{self.schema}.{self.table}" if self.table is not None else None

    def content_text(self) -> str | None:
        if self.content is None:
            return None
        try:
            return self.content.decode("utf-8")
        except UnicodeDecodeError:
            return None

    def to_dict(self) -> dict[str, Any]:
        source: dict[str, Any] = {
            "lsn": format_lsn(self.commit_lsn),
            "xid": self.xid,
            "commit_ts": self.commit_ts.isoformat().replace("+00:00", "Z")
            if self.commit_ts
            else None,
        }
        if self.origin:
            source["origin"] = self.origin
        out: dict[str, Any] = {"id": self.id, "op": self.op}
        if self.op == MESSAGE:
            text = self.content_text()
            out["prefix"] = self.prefix
            out["transactional"] = self.transactional
            if text is not None:
                out["content"] = text
            else:
                out["content_hex"] = (self.content or b"").hex()
        elif self.op == TRUNCATE:
            out["tables"] = self.tables
            out["cascade"] = self.cascade
            out["restart_identity"] = self.restart_identity
        else:
            source["schema"] = self.schema
            source["table"] = self.table
            out["key"] = self.key
            out["before"] = self.before
            out["after"] = self.after
            if self.unchanged_toast:
                out["unchanged_toast"] = self.unchanged_toast
        out["source"] = source
        return out


@dataclass
class Transaction:
    commit_lsn: int
    # Position just past the commit record: what the relay confirms once this transaction has
    # been delivered, so the server can recycle WAL up to it.
    end_lsn: int
    xid: int | None
    commit_ts: datetime | None
    events: list[ChangeEvent] = field(default_factory=list)
    origin: str | None = None
