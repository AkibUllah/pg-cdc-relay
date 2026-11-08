"""Initial snapshot: the rows that already exist when the slot is created, as ``read`` events.

A logical slot only streams changes committed after it was created. To start a consumer from
a complete copy, the slot is created over the replication protocol, which exports the snapshot
the slot starts from. Reading every published table inside that snapshot gives exactly the rows
committed before the slot's consistent point, and the stream continues with exactly the ones
committed after it: nothing is missed and nothing is read twice.

Snapshot rows go through the same value conversion and routing as streamed changes, so a row
looks the same whether it arrived as a ``read`` or as an ``insert``:

* columns come from the publication (column lists and, on 18, published generated columns);
* row filters of the publication (PostgreSQL 15+) apply;
* the key comes from the table's replica identity, like the ``Relation`` message marks it;
* values are read through the type's output function and converted by type OID.

Positions: every snapshot record uses ``consistent_point - 1`` as its commit LSN and a running
number as its sequence, so record ids are unique, Redis Stream entry ids grow, and everything
sorts before the first streamed transaction.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

from psycopg2 import sql

from cdc_relay.events import READ, ChangeEvent, Transaction
from cdc_relay.lsn import format_lsn
from cdc_relay.pgtypes import convert
from cdc_relay.routing import Record, Router

log = logging.getLogger("cdc_relay.snapshot")


@dataclass
class Column:
    name: str
    type_oid: int
    is_key: bool


@dataclass
class SnapshotTable:
    oid: int
    schema: str
    name: str
    partitioned: bool
    columns: list[Column]
    row_filter: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass
class SnapshotResult:
    consistent_point: int
    tables: dict[str, int] = field(default_factory=dict)
    records: int = 0

    @property
    def rows(self) -> int:
        return sum(self.tables.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "consistent_point": format_lsn(self.consistent_point),
            "tables": self.tables,
            "rows": self.rows,
            "records": self.records,
        }


_PUBLISHED = """
SELECT c.oid, n.nspname, c.relname, c.relkind = 'p', c.relreplident, {extra}
FROM pg_publication_tables pt
JOIN pg_namespace n ON n.nspname = pt.schemaname
JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
WHERE pt.pubname = %s
ORDER BY n.nspname, c.relname
"""

_COLUMNS = """
SELECT a.attname, a.atttypid::int, a.attgenerated <> '',
       COALESCE(a.attnum = ANY (pk.indkey), false),
       COALESCE(a.attnum = ANY (ri.indkey), false)
FROM pg_attribute a
LEFT JOIN pg_index pk ON pk.indrelid = a.attrelid AND pk.indisprimary
LEFT JOIN pg_index ri ON ri.indrelid = a.attrelid AND ri.indisreplident
WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""


def published_tables(cur, publication: str, server_version: int) -> list[SnapshotTable]:
    """The tables the publication streams, with the columns and key pgoutput would send.

    Run it inside the snapshot transaction so the catalog matches the data that is read.
    """
    has_lists = server_version >= 150000  # column lists and row filters arrived in 15
    extra = "pt.attnames, pt.rowfilter" if has_lists else "NULL::name[], NULL::text"
    cur.execute(_PUBLISHED.format(extra=extra), (publication,))
    tables = []
    for oid, schema, name, partitioned, identity, attnames, row_filter in cur.fetchall():
        cur.execute(_COLUMNS, (oid,))
        columns = []
        for col, type_oid, generated, in_pk, in_identity in cur.fetchall():
            if attnames is not None and col not in attnames:
                continue
            # Before 18 pgoutput never sends generated columns; 18 lists published ones.
            if generated and (server_version < 180000 or attnames is None):
                continue
            is_key = {"f": True, "d": in_pk, "i": in_identity}.get(identity, False)
            columns.append(Column(col, type_oid, bool(is_key)))
        tables.append(SnapshotTable(oid, schema, name, partitioned, columns, row_filter))
    return tables


def select_rows(table: SnapshotTable) -> sql.Composed:
    """Every column as text (what the output function prints), honouring the row filter.

    ``ONLY`` keeps inheritance children out (they are published separately); a partitioned
    table has no rows of its own, so it is read whole when published via its root.
    """
    columns = sql.SQL(", ").join(
        sql.SQL("{}::text").format(sql.Identifier(c.name)) for c in table.columns
    )
    query = sql.SQL("SELECT {} FROM {}{}").format(
        columns if table.columns else sql.SQL("NULL"),
        sql.SQL("" if table.partitioned else "ONLY "),
        sql.Identifier(table.schema, table.name),
    )
    if table.row_filter:
        # Catalog text from pg_get_expr, already a complete boolean expression.
        query = sql.Composed([query, sql.SQL(" WHERE "), sql.SQL(table.row_filter)])
    return query


def row_event(table: SnapshotTable, values: Sequence[str | None], position: int) -> ChangeEvent:
    after = {c.name: convert(c.type_oid, v) for c, v in zip(table.columns, values, strict=True)}
    key = {c.name: after[c.name] for c in table.columns if c.is_key}
    return ChangeEvent(
        op=READ,
        commit_lsn=position,
        seq=0,
        schema=table.schema,
        table=table.name,
        key=key or None,
        after=after,
    )


class Snapshot:
    """Reads published tables inside an exported snapshot and hands routed batches on.

    ``deliver`` receives each batch of records and must only return once the sink accepted
    them (the relay's retrying delivery); the caller decides what an interrupted snapshot means.
    """

    def __init__(
        self,
        router: Router,
        deliver: Callable[[list[Record]], None],
        *,
        consistent_point: int,
        batch_size: int = 500,
        on_rows: Callable[[str, int], None] | None = None,
    ) -> None:
        if consistent_point < 1:
            raise ValueError("consistent_point must be a real WAL position")
        self.router = router
        self.deliver = deliver
        self.position = consistent_point - 1
        self.batch_size = batch_size
        self.on_rows = on_rows
        self.result = SnapshotResult(consistent_point)
        self._seq = 0

    def route(self, table: SnapshotTable, rows: Sequence[Sequence[str | None]]) -> list[Record]:
        events = []
        for values in rows:
            event = row_event(table, values, self.position)
            event.seq = self._seq
            self._seq += 1
            events.append(event)
        tx = Transaction(
            commit_lsn=self.position,
            end_lsn=self.position + 1,
            xid=None,
            commit_ts=None,
            events=events,
        )
        records, _ = self.router.route(tx)  # read events are never rejected
        for record in records:
            # One id space for the whole snapshot: record index continues across batches.
            record.index = record.seq
        return records

    def wanted(self, table: SnapshotTable) -> bool:
        return self.router.snapshot_includes(table.qualified_name)

    def read(self, conn, tables: Sequence[SnapshotTable]) -> SnapshotResult:
        for table in tables:
            if not self.wanted(table):
                log.info("snapshot: skipping %s (not routed)", table.qualified_name)
                continue
            count = 0
            for rows in self._fetch(conn, table):
                records = self.route(table, rows)
                if records:
                    self.deliver(records)
                    self.result.records += len(records)
                count += len(rows)
                if self.on_rows:
                    self.on_rows(table.qualified_name, len(rows))
            self.result.tables[table.qualified_name] = count
            log.info("snapshot: %s, %d rows", table.qualified_name, count)
        return self.result

    def _fetch(self, conn, table: SnapshotTable) -> Iterator[list[tuple]]:
        # A server-side cursor, so a large table is never held in memory at once.
        with conn.cursor(name=f"cdc_snapshot_{table.oid}") as cur:
            cur.itersize = self.batch_size
            cur.execute(select_rows(table))
            while rows := cur.fetchmany(self.batch_size):
                if not table.columns:
                    rows = [() for _ in rows]
                yield rows


def begin(conn, snapshot_name: str) -> None:
    """Open a repeatable-read transaction on the slot's exported snapshot.

    Output settings match what the walsender prints for pgoutput, so values convert the same.
    """
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_name,))
        cur.execute(
            "SET LOCAL DateStyle = 'ISO, MDY'; SET LOCAL IntervalStyle = 'postgres'; "
            "SET LOCAL bytea_output = 'hex'; SET LOCAL extra_float_digits = 1"
        )
