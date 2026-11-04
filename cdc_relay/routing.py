"""From change events to the records sinks deliver: filtering, masking, outbox routing.

Two kinds of records come out of a transaction:

* **table changes**: one record per insert/update/delete/truncate on an included table, with
  the event envelope as value and ``{schema}.{table}`` (configurable) as topic;
* **outbox events**: the transactional outbox pattern. The application writes a domain event
  in the same transaction as its state change, either as a row in an outbox table or with
  ``pg_logical_emit_message(true, 'outbox', '{...}')`` (no table at all). The relay publishes
  the event's payload on ``<topic_prefix><aggregate_type>`` keyed by ``aggregate_id``, only
  once the transaction has committed, so consumers never see an event for a rolled-back change.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from typing import Any

from cdc_relay.events import DELETE, INSERT, MESSAGE, TRUNCATE, UPDATE, ChangeEvent, Transaction
from cdc_relay.lsn import format_lsn

TABLE_OPS = (INSERT, UPDATE, DELETE, TRUNCATE)


@dataclass
class Record:
    id: str
    topic: str
    key: str | None
    value: Any
    headers: dict[str, str]
    commit_lsn: int
    seq: int
    # Position of the record in its transaction's output (one event can make several records,
    # e.g. a truncate of two tables), so (commit_lsn, index) is unique and ordered.
    index: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "topic": self.topic,
            "key": self.key,
            "headers": self.headers,
            "value": self.value,
        }


@dataclass
class Rejected:
    event: ChangeEvent
    reason: str


@dataclass
class OutboxConfig:
    table: str | None = "public.outbox"
    message_prefix: str | None = "outbox"
    topic_prefix: str = ""
    id_column: str = "id"
    aggregate_type_column: str = "aggregate_type"
    aggregate_id_column: str = "aggregate_id"
    event_type_column: str = "event_type"
    payload_column: str = "payload"
    # Also publish the outbox table's own row changes as table-change records.
    publish_table_changes: bool = False


@dataclass
class RoutingConfig:
    include: list[str] = field(default_factory=lambda: ["*.*"])
    exclude: list[str] = field(default_factory=list)
    ops: list[str] = field(default_factory=lambda: list(TABLE_OPS))
    topic_template: str = "{schema}.{table}"
    drop_columns: dict[str, list[str]] = field(default_factory=dict)
    mask_columns: dict[str, list[str]] = field(default_factory=dict)
    # Override the replica-identity key, e.g. for REPLICA IDENTITY FULL tables where every
    # column is part of the identity.
    key_columns: dict[str, list[str]] = field(default_factory=dict)
    mask_salt: str = ""
    # Logical message prefixes (other than the outbox prefix) published on "messages.<prefix>".
    message_prefixes: list[str] = field(default_factory=list)
    outbox: OutboxConfig = field(default_factory=OutboxConfig)

    def __post_init__(self) -> None:
        unknown = set(self.ops) - set(TABLE_OPS)
        if unknown:
            raise ValueError(f"unknown ops: {', '.join(sorted(unknown))}")
        if self.mask_columns and not self.mask_salt:
            raise ValueError("mask_columns needs mask_salt (CDC_MASK_SALT), or masks are guessable")
        try:
            self.topic_template.format(schema="s", table="t")
        except (KeyError, IndexError) as exc:
            raise ValueError(
                f"topic_template may only use {{schema}} and {{table}}: {exc}"
            ) from exc


class Router:
    def __init__(self, config: RoutingConfig | None = None) -> None:
        self.config = config or RoutingConfig()

    def includes(self, qualified_table: str) -> bool:
        c = self.config
        return any(fnmatchcase(qualified_table, p) for p in c.include) and not any(
            fnmatchcase(qualified_table, p) for p in c.exclude
        )

    def route(self, tx: Transaction) -> tuple[list[Record], list[Rejected]]:
        records: list[Record] = []
        rejected: list[Rejected] = []
        for event in tx.events:
            try:
                for record in self._route_event(event):
                    record.index = len(records)
                    records.append(record)
            except _Reject as exc:
                rejected.append(Rejected(event, str(exc)))
        return records, rejected

    def _route_event(self, event: ChangeEvent) -> list[Record]:
        outbox = self.config.outbox
        if event.op == MESSAGE:
            if outbox.message_prefix and event.prefix == outbox.message_prefix:
                return [self._outbox_from_message(event)]
            if event.prefix in self.config.message_prefixes:
                return [self._record(event, f"messages.{event.prefix}", None, event.to_dict())]
            return []
        if event.op == TRUNCATE:
            if TRUNCATE not in self.config.ops:
                return []
            out = []
            for name in event.tables:
                if self.includes(name):
                    schema, table = name.split(".", 1)
                    value = event.to_dict()
                    value["tables"] = [name]
                    out.append(self._record(event, self._topic(schema, table), None, value))
            return out

        name = event.qualified_table or ""
        if outbox.table and name == outbox.table:
            records = [self._outbox_from_row(event)] if event.op == INSERT else []
            if outbox.publish_table_changes and event.op in self.config.ops:
                records.append(self._table_record(event))
            return records
        if event.op not in self.config.ops or not self.includes(name):
            return []
        return [self._table_record(event)]

    # -- table changes -------------------------------------------------------------------------

    def _table_record(self, event: ChangeEvent) -> Record:
        name = event.qualified_table or ""
        drop = set(self.config.drop_columns.get(name, ()))
        mask = set(self.config.mask_columns.get(name, ()))
        key_override = self.config.key_columns.get(name)
        value = event.to_dict()
        for part in ("before", "after"):
            if value.get(part) is not None:
                value[part] = self._clean(value[part], drop, mask)
        if key_override:
            row = event.after if event.after is not None else event.before or {}
            value["key"] = {c: row.get(c) for c in key_override}
        if value.get("key") is not None:
            value["key"] = self._clean(value["key"], set(), mask)
        key = (
            json.dumps(value["key"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            if value.get("key") is not None
            else None
        )
        return self._record(event, self._topic(event.schema or "", event.table or ""), key, value)

    def _clean(self, row: dict[str, Any], drop: set[str], mask: set[str]) -> dict[str, Any]:
        return {k: (self._mask(v) if k in mask else v) for k, v in row.items() if k not in drop}

    def _mask(self, value: Any) -> Any:
        if value is None:
            return None
        text = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
        digest = hmac.new(self.config.mask_salt.encode(), text.encode(), hashlib.sha256)
        return "hmac:" + digest.hexdigest()[:16]

    def _topic(self, schema: str, table: str) -> str:
        return self.config.topic_template.format(schema=schema, table=table)

    # -- outbox --------------------------------------------------------------------------------

    def _outbox_from_row(self, event: ChangeEvent) -> Record:
        o = self.config.outbox
        row = event.after or {}
        return self._outbox_record(
            event,
            aggregate_type=row.get(o.aggregate_type_column),
            aggregate_id=row.get(o.aggregate_id_column),
            event_type=row.get(o.event_type_column),
            payload=row.get(o.payload_column),
            outbox_id=row.get(o.id_column),
        )

    def _outbox_from_message(self, event: ChangeEvent) -> Record:
        o = self.config.outbox
        text = event.content_text()
        try:
            body = json.loads(text) if text is not None else None
        except json.JSONDecodeError as exc:
            raise _Reject(f"outbox message is not JSON: {exc}") from None
        if not isinstance(body, dict):
            raise _Reject("outbox message must be a JSON object")
        return self._outbox_record(
            event,
            aggregate_type=body.get(o.aggregate_type_column),
            aggregate_id=body.get(o.aggregate_id_column),
            event_type=body.get(o.event_type_column),
            payload=body.get(o.payload_column),
            outbox_id=body.get(o.id_column),
        )

    def _outbox_record(
        self,
        event: ChangeEvent,
        *,
        aggregate_type: Any,
        aggregate_id: Any,
        event_type: Any,
        payload: Any,
        outbox_id: Any,
    ) -> Record:
        if not isinstance(aggregate_type, str) or not aggregate_type:
            raise _Reject("outbox event without aggregate_type")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                pass  # a text payload column that holds plain text is published as is
        headers = {"aggregate_type": aggregate_type, "source_lsn": format_lsn(event.commit_lsn)}
        if event_type is not None:
            headers["event_type"] = str(event_type)
        if outbox_id is not None:
            headers["outbox_id"] = str(outbox_id)
        key = str(aggregate_id) if aggregate_id is not None else None
        return self._record(
            event, f"{self.config.outbox.topic_prefix}{aggregate_type}", key, payload, headers
        )

    def _record(
        self,
        event: ChangeEvent,
        topic: str,
        key: str | None,
        value: Any,
        headers: dict[str, str] | None = None,
    ) -> Record:
        return Record(
            id=event.id,
            topic=topic,
            key=key,
            value=value,
            headers=headers or {"op": event.op},
            commit_lsn=event.commit_lsn,
            seq=event.seq,
        )


class _Reject(Exception):
    pass
