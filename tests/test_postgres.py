"""End to end against a real server: TEST_PG_DSN must point at PostgreSQL with wal_level=logical.

    docker run -d -p 55433:5432 -e POSTGRES_PASSWORD=postgres postgres:16-alpine \\
        -c wal_level=logical
    TEST_PG_DSN=postgresql://postgres:postgres@localhost:55433/postgres pytest -m postgres
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

pytestmark = pytest.mark.postgres

DSN = os.environ.get("TEST_PG_DSN")
if not DSN:
    pytest.skip("TEST_PG_DSN not set", allow_module_level=True)

import psycopg2  # noqa: E402
import psycopg2.errors  # noqa: E402

from cdc_relay import admin, demo  # noqa: E402
from cdc_relay.checkpoint import FileCheckpoint  # noqa: E402
from cdc_relay.config import load_settings  # noqa: E402
from cdc_relay.lsn import parse_lsn  # noqa: E402
from cdc_relay.pipeline import Relay  # noqa: E402
from cdc_relay.routing import Router  # noqa: E402
from cdc_relay.source import ReplicationSource  # noqa: E402


class ListSink:
    name = "list"

    def __init__(self) -> None:
        self.records = []
        self.lock = threading.Lock()

    def send(self, records) -> None:
        with self.lock:
            self.records.extend(records)

    def close(self) -> None:
        pass

    def outbox_keys(self) -> list[tuple[str, str]]:
        with self.lock:
            return [
                (r.headers["event_type"], r.key) for r in self.records if "event_type" in r.headers
            ]


def wait_for(predicate, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError("timed out waiting for the relay")


@pytest.fixture
def conn():
    c = psycopg2.connect(DSN)
    demo.setup(c)
    yield c
    c.close()


@pytest.fixture
def names(conn):
    suffix = uuid.uuid4().hex[:8]
    slot, pub = f"test_slot_{suffix}", f"test_pub_{suffix}"
    yield slot, pub
    conn.rollback()
    deadline = time.monotonic() + 15
    while True:  # a terminated walsender can hold the slot for a moment
        try:
            admin.drop_slot(conn, slot)
            break
        except psycopg2.errors.ObjectInUse:
            conn.rollback()
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)
    admin.drop_publication(conn, pub)


def settings(slot: str, pub: str, tmp_path):
    s = load_settings(env={})
    s.dsn, s.slot, s.publication = DSN, slot, pub
    s.publication_tables = ["public.parcels", "public.riders", "public.outbox"]
    s.checkpoint_path = str(tmp_path / f"{slot}.checkpoint.json")
    s.batch_max_wait = 0.05
    s.status_interval = 1.0
    s.lag_check_interval = 0.5
    return s


class Running:
    """A ReplicationSource on a background thread."""

    def __init__(self, s, sink) -> None:
        self.relay = Relay(
            Router(s.routing),
            sink,
            checkpoint=FileCheckpoint(s.checkpoint_path, s.slot),
            batch_max_wait=s.batch_max_wait,
        )
        self.source = ReplicationSource(s, self.relay)
        self.source.prepare()
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        wait_for(lambda: self.source.connected)

    def _run(self) -> None:
        try:
            self.source.run_forever()
        except BaseException as exc:  # surfaced in stop()
            self.error = exc

    def stop(self) -> None:
        self.relay.stop.set()
        self.thread.join(timeout=10)
        assert not self.thread.is_alive()
        if self.error:
            raise self.error


def test_server_checks_and_idempotent_setup(conn, names) -> None:
    slot, pub = names
    assert admin.check_server(conn)["wal_level"] == "logical"
    assert admin.ensure_publication(conn, pub, ["public.parcels"]) is True
    assert admin.ensure_publication(conn, pub, ["public.parcels"]) is False
    assert admin.ensure_slot(conn, slot) is True
    assert admin.ensure_slot(conn, slot) is False
    status = admin.slot_status(conn, slot)
    assert status["active"] is False and status["confirmed_flush_lsn"]
    assert status["pending_bytes"] >= 0
    assert admin.slot_status(conn, "no_such_slot") is None


def test_peek_does_not_consume(conn, names) -> None:
    slot, pub = names
    admin.ensure_publication(conn, pub, ["public.parcels", "public.outbox"])
    admin.ensure_slot(conn, slot)
    demo.run_script(conn, seed=1, parcels=1)
    first = list(admin.peek_changes(conn, slot, pub))
    second = list(admin.peek_changes(conn, slot, pub))
    assert first == second and len(first) > 5
    assert first[0][2][:1] == b"B"


def test_streams_commits_only_and_confirms_the_slot(conn, names, tmp_path) -> None:
    slot, pub = names
    s = settings(slot, pub, tmp_path)
    sink = ListSink()
    run = Running(s, sink)
    try:
        codes = demo.run_script(conn, seed=3, parcels=2)["parcels"]
        wait_for(lambda: ("ParcelCancelled", codes[-1]) in sink.outbox_keys())
    finally:
        run.stop()
    keys = sink.outbox_keys()
    assert keys[:2] == [("ParcelBooked", codes[0]), ("ParcelBooked", codes[1])]
    assert ("RiderAssigned", "1") in keys
    assert all("SC-ROLLED-BACK" not in str(r.value) for r in sink.records)
    status = admin.slot_status(conn, slot)
    assert parse_lsn(status["confirmed_flush_lsn"]) >= run.relay.confirmed_lsn > 0
    assert run.relay.metrics.get("cdc_slot_lag_bytes") >= 0


def test_restart_resumes_without_duplicates(conn, names, tmp_path) -> None:
    slot, pub = names
    s = settings(slot, pub, tmp_path)
    first = ListSink()
    run = Running(s, first)
    try:
        a = demo.run_script(conn, seed=4, parcels=1)["parcels"][0]
        wait_for(lambda: ("ParcelCancelled", a) in first.outbox_keys())
    finally:
        run.stop()
    b = demo.run_script(conn, seed=5, parcels=1)["parcels"][0]  # written while stopped
    second = ListSink()
    run = Running(s, second)
    try:
        wait_for(lambda: ("ParcelCancelled", b) in second.outbox_keys())
    finally:
        run.stop()
    assert all(key != a for _, key in second.outbox_keys())
    ids = [r.id for r in first.records + second.records]
    assert len(ids) == len(set(ids))


def test_reconnects_after_the_walsender_is_killed(conn, names, tmp_path) -> None:
    slot, pub = names
    s = settings(slot, pub, tmp_path)
    sink = ListSink()
    run = Running(s, sink)
    try:
        a = demo.run_script(conn, seed=6, parcels=1)["parcels"][0]
        wait_for(lambda: ("ParcelCancelled", a) in sink.outbox_keys())
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                "WHERE slot_name = %s",
                (slot,),
            )
        conn.commit()
        b = demo.run_script(conn, seed=7, parcels=1)["parcels"][0]
        wait_for(lambda: ("ParcelCancelled", b) in sink.outbox_keys(), timeout=30)
    finally:
        run.stop()
    assert run.source.reconnects >= 1
    ids = [r.id for r in sink.records]
    assert len(ids) == len(set(ids))


# -- initial snapshot --------------------------------------------------------------------------


@pytest.fixture
def table(conn):
    name = f"snap_{uuid.uuid4().hex[:8]}"
    yield name
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}")
    conn.commit()


def snapshot_settings(slot, pub, tmp_path, *tables):
    s = settings(slot, pub, tmp_path)
    s.publication_tables = [f"public.{t}" for t in tables]
    s.snapshot = True
    s.batch_max_records = 100
    return s


def server_version(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SHOW server_version_num")
        return int(cur.fetchone()[0])


def apply(records, table_name):
    """Fold read/insert/update/delete records into the current rows, by id."""
    rows = {}
    for r in records:
        if r.topic != f"public.{table_name}":
            continue
        value = r.value
        if value["op"] == "delete":
            rows.pop(value["key"]["id"], None)
        else:
            rows[value["after"]["id"]] = value["after"]
    return rows


def test_snapshot_and_stream_meet_without_gaps_or_duplicates(conn, names, table, tmp_path):
    slot, pub = names
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id int PRIMARY KEY, n int NOT NULL)")
        cur.execute(f"INSERT INTO {table} SELECT g, 0 FROM generate_series(1, 5000) g")
    conn.commit()

    stop_writing = threading.Event()

    def writer() -> None:
        w = psycopg2.connect(DSN)
        w.autocommit = True
        i = 0
        with w.cursor() as cur:
            while not stop_writing.is_set():
                i += 1
                cur.execute(f"UPDATE {table} SET n = n + 1 WHERE id = %s", (i % 5000 + 1,))
                cur.execute(f"INSERT INTO {table} VALUES (%s, 0)", (10000 + i,))
                if i % 3 == 0:
                    cur.execute(f"DELETE FROM {table} WHERE id = %s", (i % 5000 + 1,))
        w.close()

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    time.sleep(0.2)
    sink = ListSink()
    run = Running(snapshot_settings(slot, pub, tmp_path, table), sink)
    try:
        time.sleep(0.5)
        stop_writing.set()
        thread.join(timeout=10)
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {table} VALUES (-1, -1)")  # sentinel, after every write
        conn.commit()
        wait_for(lambda: any(r.key == '{"id":-1}' for r in list(sink.records)), timeout=30)
    finally:
        run.stop()

    reads = [r for r in sink.records if r.value["op"] == "read"]
    changes = [r for r in sink.records if r.value["op"] != "read"]
    assert reads and changes  # the writer was busy on both sides of the consistent point
    snapshot = run.source.snapshot
    assert snapshot["state"] == "done" and snapshot["rows"] == len(reads)
    point = parse_lsn(snapshot["consistent_point"])
    assert all(r.commit_lsn < point for r in reads)
    assert all(r.commit_lsn >= point for r in changes)
    assert sink.records.index(reads[-1]) < sink.records.index(changes[0])
    ids = [r.id for r in sink.records]
    assert len(ids) == len(set(ids))
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, n FROM {table}")
        expected = dict(cur.fetchall())
    folded = apply(sink.records, table)
    assert {k: v["n"] for k, v in folded.items()} == expected
    assert run.relay.metrics.total("cdc_snapshot_rows_total") == len(reads)


def test_snapshot_values_match_streamed_values(conn, names, table, tmp_path) -> None:
    slot, pub = names
    generated = "GENERATED ALWAYS AS (qty * 2) STORED"
    columns = f"""
        id int PRIMARY KEY, code text, qty int, doubled int {generated},
        price numeric(12, 2), weight float8, paid boolean, ref uuid,
        booked_at timestamptz, due date, meta jsonb, tags text[], blob bytea,
        note varchar(40)
    """
    values = """
        %s, 'SC-ঢাকা-1', 3, 1250.50, 2.5, true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
        '2026-11-08 09:30:00+06', '2026-11-09', '{"fragile": true, "n": [1, 2]}',
        ARRAY['a', 'b c', NULL], '\\xdeadbeef', NULL
    """
    insert = f"INSERT INTO {table} (id, code, qty, price, weight, paid, ref, booked_at, due, "
    insert += f"meta, tags, blob, note) VALUES ({values})"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} ({columns})")
        cur.execute(insert, (1,))
    conn.commit()
    sink = ListSink()
    run = Running(snapshot_settings(slot, pub, tmp_path, table), sink)
    try:
        with conn.cursor() as cur:
            cur.execute("SET TimeZone = 'Asia/Dhaka'")
            cur.execute(insert, (2,))
        conn.commit()
        wait_for(lambda: len(sink.records) == 2)
    finally:
        run.stop()
    read, insert_record = sink.records
    assert (read.value["op"], insert_record.value["op"]) == ("read", "insert")
    a, b = dict(read.value["after"]), dict(insert_record.value["after"])
    assert (a.pop("id"), b.pop("id")) == (1, 2)
    assert a == b
    assert a["price"] == "1250.50" and a["booked_at"] == "2026-11-08T03:30:00Z"
    assert a["tags"] == ["a", "b c", None] and a["blob"] == "deadbeef"
    # generated columns are only streamed when PostgreSQL 18 publishes them
    assert "doubled" not in a
    assert read.value["key"] == {"id": 1} and read.value["source"]["snapshot"] is True


def test_unfinished_snapshot_is_taken_again_with_a_new_slot(conn, names, table, tmp_path):
    slot, pub = names
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id int PRIMARY KEY)")
        cur.execute(f"INSERT INTO {table} SELECT generate_series(1, 300)")
    conn.commit()
    s = snapshot_settings(slot, pub, tmp_path, table)

    class FailingSink(ListSink):
        def send(self, records) -> None:
            if len(self.records) >= 100:
                raise RuntimeError("process killed mid-snapshot")
            super().send(records)

    relay = Relay(
        Router(s.routing), FailingSink(), checkpoint=FileCheckpoint(s.checkpoint_path, slot)
    )
    with pytest.raises(RuntimeError, match="killed"):
        ReplicationSource(s, relay).prepare()
    assert admin.slot_status(conn, slot) is None  # dropped, so nothing streams past the gap
    assert FileCheckpoint(s.checkpoint_path, slot).snapshot_state() == "pending"

    # a hard crash leaves the slot behind: the pending state still forces a fresh snapshot
    admin.ensure_slot(conn, slot)
    old = admin.slot_status(conn, slot)["confirmed_flush_lsn"]
    sink = ListSink()
    run = Running(s, sink)
    run.stop()
    assert len(sink.records) == 300 and {r.value["op"] for r in sink.records} == {"read"}
    assert parse_lsn(admin.slot_status(conn, slot)["confirmed_flush_lsn"]) > parse_lsn(old)
    assert FileCheckpoint(s.checkpoint_path, slot).snapshot_state() == "done"

    # and once done, a restart streams on without reading the table again
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {table} VALUES (301)")
    conn.commit()
    again = ListSink()
    run = Running(s, again)
    try:
        wait_for(lambda: len(again.records) == 1)
    finally:
        run.stop()
    assert again.records[0].value["op"] == "insert"


def test_snapshot_follows_row_filters_and_column_lists(conn, names, table, tmp_path) -> None:
    if server_version(conn) < 150000:
        pytest.skip("row filters and column lists need PostgreSQL 15")
    slot, pub = names
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id int PRIMARY KEY, status text, secret text)")
        cur.execute(
            f"INSERT INTO {table} VALUES (1, 'booked', 'x'), (2, 'draft', 'y'), (3, 'booked', 'z')"
        )
        cur.execute(
            f"CREATE PUBLICATION {pub} FOR TABLE {table} (id, status) WHERE (status <> 'draft')"
        )
    conn.commit()
    sink = ListSink()
    run = Running(snapshot_settings(slot, pub, tmp_path, table), sink)
    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {table} VALUES (4, 'booked', 'w'), (5, 'draft', 'v')")
        conn.commit()
        wait_for(lambda: len(sink.records) == 3)
        time.sleep(0.3)
    finally:
        run.stop()
    seen = [(r.value["op"], r.value["after"]) for r in sink.records]
    assert seen == [
        ("read", {"id": 1, "status": "booked"}),
        ("read", {"id": 3, "status": "booked"}),
        ("insert", {"id": 4, "status": "booked"}),
    ]


def test_slot_create_leaves_a_snapshot_slot_to_run(conn, names, monkeypatch, capsys) -> None:
    from cdc_relay import cli

    slot, pub = names
    for var, value in {
        "CDC_DSN": DSN,
        "CDC_SLOT": slot,
        "CDC_PUBLICATION": pub,
        "CDC_SNAPSHOT": "true",
    }.items():
        monkeypatch.setenv(var, value)
    assert cli.main(["slot", "create"]) == 0
    out = capsys.readouterr().out
    assert f"publication {pub}: created" in out and "left to `cdc-relay run`" in out
    assert admin.slot_status(conn, slot) is None
