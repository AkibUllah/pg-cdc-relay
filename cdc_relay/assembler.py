"""Turns the flat pgoutput message stream into committed transactions of change events.

pgoutput only streams a transaction once it has committed, framed by Begin and Commit, and
sends a Relation message before the first change to a table in each session (and again after
the table's schema changes). The assembler keeps that relation cache and maps tuple positions
to column names and typed values.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from cdc_relay import pgoutput as pg
from cdc_relay.events import DELETE, INSERT, MESSAGE, TRUNCATE, UPDATE, ChangeEvent, Transaction
from cdc_relay.pgtypes import convert


class StreamError(RuntimeError):
    """The message stream is out of order, e.g. a change for a relation never described."""


@dataclass
class _Open:
    begin: pg.Begin
    events: list[ChangeEvent] = field(default_factory=list)
    origin: str | None = None


class TransactionAssembler:
    def __init__(self) -> None:
        self.relations: dict[int, pg.Relation] = {}
        self._open: _Open | None = None

    @property
    def in_transaction(self) -> bool:
        return self._open is not None

    def feed(self, message: pg.Message, wal_end: int = 0) -> list[Transaction]:
        """Consume one decoded message; return the transactions it completed (usually none)."""
        if isinstance(message, pg.Relation):
            self.relations[message.oid] = message
            return []
        if isinstance(message, pg.Type):
            return []
        if isinstance(message, pg.Begin):
            if self._open is not None:
                raise StreamError(f"Begin of xid {message.xid} inside xid {self._open.begin.xid}")
            self._open = _Open(begin=message)
            return []
        if isinstance(message, pg.LogicalMessage) and not message.transactional:
            event = ChangeEvent(
                op=MESSAGE,
                commit_lsn=message.lsn,
                seq=0,
                prefix=message.prefix,
                content=message.content,
                transactional=False,
            )
            return [
                Transaction(
                    commit_lsn=message.lsn,
                    end_lsn=max(message.lsn, wal_end),
                    xid=None,
                    commit_ts=None,
                    events=[event],
                )
            ]

        tx = self._open
        if tx is None:
            raise StreamError(f"{type(message).__name__} outside a transaction")
        if isinstance(message, pg.Commit):
            self._open = None
            if message.commit_lsn != tx.begin.final_lsn:
                raise StreamError(
                    f"Commit LSN {message.commit_lsn:X} does not match Begin {tx.begin.final_lsn:X}"
                )
            return [
                Transaction(
                    commit_lsn=message.commit_lsn,
                    end_lsn=message.end_lsn,
                    xid=tx.begin.xid,
                    commit_ts=message.commit_ts,
                    events=tx.events,
                    origin=tx.origin,
                )
            ]
        if isinstance(message, pg.Origin):
            tx.origin = message.name
            return []

        event = self._event(message, tx)
        event.seq = len(tx.events)
        tx.events.append(event)
        return []

    def _event(self, message: pg.Message, tx: _Open) -> ChangeEvent:
        common: dict[str, Any] = {
            "commit_lsn": tx.begin.final_lsn,
            "seq": 0,
            "xid": tx.begin.xid,
            "commit_ts": tx.begin.commit_ts,
            "origin": tx.origin,
        }
        if isinstance(message, pg.LogicalMessage):
            return ChangeEvent(op=MESSAGE, prefix=message.prefix, content=message.content, **common)
        if isinstance(message, pg.Truncate):
            names = [self._relation(oid).qualified_name for oid in message.relation_oids]
            return ChangeEvent(
                op=TRUNCATE,
                tables=names,
                cascade=message.cascade,
                restart_identity=message.restart_identity,
                **common,
            )

        rel = self._relation(message.relation_oid)  # type: ignore[union-attr]
        common.update(schema=rel.namespace, table=rel.name)
        if isinstance(message, pg.Insert):
            after, unchanged = _row(rel, message.new)
            return ChangeEvent(op=INSERT, key=_key(rel, after), after=after, **common)
        if isinstance(message, pg.Update):
            before = None
            if message.old is not None:
                before, _ = _row(rel, message.old, only_keys=message.old_kind == "K")
            after, unchanged = _row(rel, message.new)
            if unchanged and message.old_kind == "O" and before is not None:
                for name in unchanged:  # the full old row has the value the server left out
                    after[name] = before.get(name)
                unchanged = []
            return ChangeEvent(
                op=UPDATE,
                key=_key(rel, after),
                before=before,
                after=after,
                unchanged_toast=unchanged,
                **common,
            )
        if isinstance(message, pg.Delete):
            before, _ = _row(rel, message.old, only_keys=message.old_kind == "K")
            return ChangeEvent(op=DELETE, key=_key(rel, before), before=before, **common)
        raise StreamError(f"unexpected {type(message).__name__} inside a transaction")

    def _relation(self, oid: int) -> pg.Relation:
        try:
            return self.relations[oid]
        except KeyError:
            raise StreamError(f"change for relation {oid} before its Relation message") from None


def _row(
    rel: pg.Relation, values: tuple[pg.TupleValue, ...], *, only_keys: bool = False
) -> tuple[dict[str, Any], list[str]]:
    if len(values) != len(rel.columns):
        raise StreamError(
            f"{rel.qualified_name}: tuple has {len(values)} values, relation has "
            f"{len(rel.columns)} columns"
        )
    row: dict[str, Any] = {}
    unchanged: list[str] = []
    for column, value in zip(rel.columns, values, strict=True):
        if only_keys and not column.is_key:
            continue
        if value is pg.UNCHANGED_TOAST:
            unchanged.append(column.name)
        elif isinstance(value, bytes):
            row[column.name] = value.hex()
        else:
            row[column.name] = convert(column.type_oid, value)  # type: ignore[arg-type]
    return row, unchanged


def _key(rel: pg.Relation, row: dict[str, Any]) -> dict[str, Any] | None:
    key = {c.name: row.get(c.name) for c in rel.columns if c.is_key}
    return key or None
