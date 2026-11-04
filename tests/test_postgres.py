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
