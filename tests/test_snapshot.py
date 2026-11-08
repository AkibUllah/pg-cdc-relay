"""Initial snapshot logic without a server: catalog interpretation, conversion, ids, batching."""

from __future__ import annotations

import pytest

from cdc_relay.lsn import format_lsn
from cdc_relay.routing import Router, RoutingConfig
from cdc_relay.sinks.redis_stream import entry_id
from cdc_relay.snapshot import (
    Column,
    Snapshot,
    SnapshotTable,
    published_tables,
    row_event,
)

INT4, TEXT, NUMERIC, TIMESTAMPTZ, JSONB, BOOL = 23, 25, 1700, 1184, 3802, 16

PARCELS = SnapshotTable(
    oid=16400,
    schema="public",
    name="parcels",
    partitioned=False,
    columns=[
        Column("id", INT4, True),
        Column("status", TEXT, False),
        Column("cod_amount", NUMERIC, False),
        Column("booked_at", TIMESTAMPTZ, False),
        Column("meta", JSONB, False),
    ],
)
ROW = ("1", "booked", "350.50", "2026-11-08 09:30:00+06", '{"fragile": true}')


class CatalogCursor:
    """Answers the two catalog queries of published_tables from canned rows."""

    def __init__(self, tables, columns) -> None:
        self.tables = tables
        self.columns = columns
        self.executed: list[tuple[str, tuple]] = []
        self._result: list = []

    def execute(self, query, params=()) -> None:
        self.executed.append((query, params))
        if "pg_publication_tables" in query:
            self._result = self.tables
        else:
            self._result = self.columns[params[0]]

    def fetchall(self):
        return self._result


class ScriptedConn:
    """A connection whose named cursors return canned rows per table oid."""

    def __init__(self, rows_by_oid) -> None:
        self.rows_by_oid = rows_by_oid
        self.cursors: list[str] = []

    def cursor(self, name=None):
        self.cursors.append(name)
        oid = int(name.rsplit("_", 1)[1])
        return _Cursor(list(self.rows_by_oid.get(oid, [])))


class _Cursor:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.itersize = 0
        self.query = None

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        pass

    def execute(self, query) -> None:
        self.query = query

    def fetchmany(self, size):
        batch, self.rows = self.rows[:size], self.rows[size:]
        return batch


# -- catalog -----------------------------------------------------------------------------------

#        name     type    generated in_pk  in_replident
COLS = [
    ("id", INT4, False, True, False),
    ("code", TEXT, False, False, True),
    ("status", TEXT, False, False, False),
    ("total", NUMERIC, True, False, False),
]


def test_default_identity_marks_primary_key_columns_and_skips_generated_before_18() -> None:
    cur = CatalogCursor([(1, "public", "parcels", False, "d", None, None)], {1: COLS})
    [table] = published_tables(cur, "pub", 140000)
    assert [(c.name, c.is_key) for c in table.columns] == [
        ("id", True),
        ("code", False),
        ("status", False),
    ]
    assert table.row_filter is None
    assert "NULL::name[]" in cur.executed[0][0]  # no attnames/rowfilter columns on 14
    assert cur.executed[0][1] == ("pub",)


@pytest.mark.parametrize(
    ("identity", "keys"),
    [("f", ["id", "code", "status"]), ("i", ["code"]), ("n", [])],
)
def test_replica_identity_full_index_and_nothing(identity, keys) -> None:
    cur = CatalogCursor([(1, "public", "parcels", False, identity, None, None)], {1: COLS})
    [table] = published_tables(cur, "pub", 160000)
    assert [c.name for c in table.columns if c.is_key] == keys


def test_column_lists_and_row_filters_from_15() -> None:
    row = (1, "public", "parcels", False, "d", ["id", "status", "total"], "(status <> 'draft')")
    cur = CatalogCursor([row], {1: COLS})
    [table] = published_tables(cur, "pub", 160000)
    assert [c.name for c in table.columns] == ["id", "status"]  # generated never before 18
    assert table.row_filter == "(status <> 'draft')"
    assert "pt.attnames, pt.rowfilter" in cur.executed[0][0]


def test_published_generated_columns_on_18() -> None:
    cur = CatalogCursor([(1, "public", "parcels", True, "d", ["id", "total"], None)], {1: COLS})
    [table] = published_tables(cur, "pub", 180000)
    assert [c.name for c in table.columns] == ["id", "total"]
    assert table.partitioned is True


# -- rows --------------------------------------------------------------------------------------


def test_row_values_convert_like_streamed_values() -> None:
    event = row_event(PARCELS, ROW, position=0x16B3747)
    assert event.op == "read"
    assert event.after == {
        "id": 1,
        "status": "booked",
        "cod_amount": "350.50",
        "booked_at": "2026-11-08T03:30:00Z",
        "meta": {"fragile": True},
    }
    assert event.key == {"id": 1}
    assert event.before is None and event.xid is None and event.commit_ts is None
    assert event.qualified_table == "public.parcels"


def test_null_values_and_tables_without_a_key() -> None:
    table = SnapshotTable(1, "public", "log", False, [Column("line", TEXT, False)])
    event = row_event(table, (None,), position=10)
    assert event.after == {"line": None}
    assert event.key is None


def test_row_width_must_match_the_columns() -> None:
    with pytest.raises(ValueError):
        row_event(PARCELS, ("1",), position=10)


# -- ids and batches ---------------------------------------------------------------------------


def test_ids_sort_before_the_consistent_point_and_never_repeat() -> None:
    delivered = []
    snap = Snapshot(Router(), delivered.extend, consistent_point=0x16B3748, batch_size=2)
    rows = [(str(i), "booked", "1.00", None, None) for i in range(5)]
    result = snap.read(ScriptedConn({PARCELS.oid: rows}), [PARCELS])
    assert result.tables == {"public.parcels": 5}
    assert result.rows == 5 and result.records == 5
    assert [r.id for r in delivered] == [f"0/16B3747:{i}" for i in range(5)]
    ids = [entry_id(r) for r in delivered]
    assert ids == [f"{0x16B3747}-{i}" for i in range(5)]
    assert all(r.commit_lsn < 0x16B3748 for r in delivered)


def test_batches_follow_the_batch_size_across_tables() -> None:
    riders = SnapshotTable(16500, "public", "riders", False, [Column("id", INT4, True)])
    batches = []
    seen = []
    snap = Snapshot(
        Router(),
        batches.append,
        consistent_point=100,
        batch_size=3,
        on_rows=lambda table, n: seen.append((table, n)),
    )
    conn = ScriptedConn(
        {
            PARCELS.oid: [(str(i), "x", None, None, None) for i in range(4)],
            riders.oid: [("1",), ("2",)],
        }
    )
    result = snap.read(conn, [PARCELS, riders])
    assert [len(b) for b in batches] == [3, 1, 2]
    assert seen == [("public.parcels", 3), ("public.parcels", 1), ("public.riders", 2)]
    assert result.to_dict() == {
        "consistent_point": format_lsn(100),
        "tables": {"public.parcels": 4, "public.riders": 2},
        "rows": 6,
        "records": 6,
    }
    # the sequence continues across tables, so ids stay unique in the snapshot
    assert [r.seq for b in batches for r in b] == list(range(6))
    assert conn.cursors == ["cdc_snapshot_16400", "cdc_snapshot_16500"]


def test_tables_outside_the_routing_are_not_read() -> None:
    outbox = SnapshotTable(16600, "public", "outbox", False, [Column("id", INT4, True)])
    config = RoutingConfig(exclude=["public.riders"])
    riders = SnapshotTable(16500, "public", "riders", False, [Column("id", INT4, True)])
    conn = ScriptedConn({outbox.oid: [("1",)], riders.oid: [("1",)], PARCELS.oid: []})
    batches = []
    result = Snapshot(Router(config), batches.append, consistent_point=100).read(
        conn, [outbox, PARCELS, riders]
    )
    assert conn.cursors == ["cdc_snapshot_16400"]
    assert result.tables == {"public.parcels": 0}
    assert batches == []  # an empty table delivers nothing


def test_masking_applies_to_snapshot_rows() -> None:
    config = RoutingConfig(mask_columns={"public.parcels": ["status"]}, mask_salt="pepper")
    batches = []
    snap = Snapshot(Router(config), batches.append, consistent_point=100)
    snap.read(ScriptedConn({PARCELS.oid: [ROW]}), [PARCELS])
    [[record]] = batches
    assert record.value["after"]["status"].startswith("hmac:")


def test_consistent_point_must_be_a_position() -> None:
    with pytest.raises(ValueError):
        Snapshot(Router(), print, consistent_point=0)
