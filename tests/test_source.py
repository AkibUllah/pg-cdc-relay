"""Slot setup decisions around the initial snapshot, with the database calls replaced."""

from __future__ import annotations

import pytest

from cdc_relay import admin
from cdc_relay import source as source_module
from cdc_relay.checkpoint import FileCheckpoint, MemoryCheckpoint
from cdc_relay.config import load_settings
from cdc_relay.pipeline import Relay, RelayStopped
from cdc_relay.routing import Router
from cdc_relay.server import heartbeat_health
from cdc_relay.source import ReplicationSource


class Sink:
    name = "list"

    def __init__(self) -> None:
        self.records = []

    def send(self, records) -> None:
        self.records.extend(records)

    def close(self) -> None:
        pass


class Conn:
    def close(self) -> None:
        pass


class FakeServer:
    """Stands in for the admin calls and remembers what happened to the slot."""

    def __init__(self, monkeypatch, *, slot_exists: bool) -> None:
        self.slot_exists = slot_exists
        self.calls: list[str] = []
        monkeypatch.setattr(admin, "check_server", lambda conn: {"server_version_num": 170000})
        monkeypatch.setattr(admin, "ensure_publication", lambda conn, name, tables: False)
        monkeypatch.setattr(admin, "slot_status", self.slot_status)
        monkeypatch.setattr(admin, "drop_slot", self.drop_slot)
        monkeypatch.setattr(admin, "ensure_slot", self.ensure_slot)

    def slot_status(self, conn, slot):
        return {"slot": slot, "pending_bytes": 0} if self.slot_exists else None

    def drop_slot(self, conn, slot) -> bool:
        self.calls.append("drop")
        existed, self.slot_exists = self.slot_exists, False
        return existed

    def ensure_slot(self, conn, slot) -> bool:
        self.calls.append("ensure_slot")
        created, self.slot_exists = not self.slot_exists, True
        return created


def make_source(checkpoint, *, snapshot=True, take=None):
    s = load_settings(env={})
    s.snapshot = snapshot
    relay = Relay(Router(s.routing), Sink(), checkpoint=checkpoint)
    src = ReplicationSource(s, relay, connect=lambda *a, **k: Conn())
    if take is not None:
        src.take_snapshot = take  # type: ignore[method-assign]
    return src


def test_without_snapshot_the_slot_is_created_with_the_sql_function(monkeypatch) -> None:
    server = FakeServer(monkeypatch, slot_exists=False)
    taken = []
    src = make_source(MemoryCheckpoint(), snapshot=False, take=taken.append)
    src.prepare()
    assert server.calls == ["ensure_slot"] and taken == []


def test_a_new_slot_is_created_with_a_snapshot(monkeypatch) -> None:
    server = FakeServer(monkeypatch, slot_exists=False)
    checkpoint = MemoryCheckpoint()
    states = []

    def take(version):
        states.append((checkpoint.snapshot_state(), version))
        server.slot_exists = True
        checkpoint.save_snapshot("done", 0x500)

    src = make_source(checkpoint, take=take)
    src.prepare()
    assert states == [("pending", 170000)]  # pending is written before the slot exists
    assert server.calls == []  # never the SQL function: it cannot export a snapshot
    assert checkpoint.snapshot_state() == "done"


def test_an_existing_slot_is_not_snapshotted_again(monkeypatch) -> None:
    server = FakeServer(monkeypatch, slot_exists=True)
    taken = []
    for state in ("done", None):
        src = make_source(MemoryCheckpoint(snapshot=state), take=taken.append)
        src.prepare()
    assert taken == [] and server.calls == []


def test_an_unfinished_snapshot_is_redone_with_a_new_slot(monkeypatch, tmp_path) -> None:
    server = FakeServer(monkeypatch, slot_exists=True)
    path = str(tmp_path / "cp.json")
    FileCheckpoint(path, "cdc_relay").save_snapshot("pending")
    taken = []

    def take(version):
        taken.append(server.slot_exists)
        server.slot_exists = True

    make_source(FileCheckpoint(path, "cdc_relay"), take=take).prepare()
    assert server.calls == ["drop"]
    assert taken == [False]  # the old slot is gone before the new snapshot starts


@pytest.mark.parametrize("error", [RuntimeError("sink exploded"), RelayStopped("stop")])
def test_a_failed_snapshot_drops_its_slot_and_stays_pending(monkeypatch, error) -> None:
    server = FakeServer(monkeypatch, slot_exists=False)
    checkpoint = MemoryCheckpoint()

    def take(version):
        server.slot_exists = True
        raise error

    with pytest.raises(type(error)):
        make_source(checkpoint, take=take).prepare()
    assert server.calls == ["drop"] and not server.slot_exists
    assert checkpoint.snapshot_state() == "pending"


def test_dropping_after_a_failure_tolerates_a_database_error(monkeypatch) -> None:
    import psycopg2

    FakeServer(monkeypatch, slot_exists=False)

    def broken_drop(conn, slot):
        raise psycopg2.OperationalError("server closed the connection")

    monkeypatch.setattr(admin, "drop_slot", broken_drop)

    def take(version):
        raise RuntimeError("snapshot failed")

    with pytest.raises(RuntimeError, match="snapshot failed"):
        make_source(MemoryCheckpoint(), take=take).prepare()


def test_progress_refreshes_the_heartbeat_counts_rows_and_honours_stop(monkeypatch) -> None:
    src = make_source(MemoryCheckpoint())
    times = iter([100.0, 200.0])
    monkeypatch.setattr(source_module.time, "monotonic", lambda: next(times))
    src._snapshot_progress("public.parcels", 500)
    assert src.heartbeat == 100.0
    assert src.relay.metrics.get("cdc_snapshot_rows_total", table="public.parcels") == 500
    src.stop.set()
    with pytest.raises(RelayStopped):
        src._snapshot_progress("public.parcels", 1)


def test_health_counts_a_running_snapshot_as_alive() -> None:
    src = make_source(MemoryCheckpoint())
    healthy = heartbeat_health(src, max_age=60)
    assert healthy() is False  # neither streaming nor snapshotting
    src.snapshotting = True
    assert healthy() is True
    src.heartbeat -= 120
    assert healthy() is False  # a stuck snapshot still turns unhealthy


def test_status_reports_the_snapshot() -> None:
    src = make_source(MemoryCheckpoint())
    assert src.status()["snapshot"] is None
    src.snapshot = {"state": "done", "rows": 3}
    assert src.status()["snapshot"] == {"state": "done", "rows": 3}
