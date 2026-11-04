"""Replays a stream captured from PostgreSQL 16 (``cdc-relay capture`` after ``cdc-relay demo``).

The fixture is the real walsender output for one demo round, so these tests pin the decoder and
the routing against what the server actually sends, not against hand-built bytes.
"""

import json
from pathlib import Path

import fakeredis

from cdc_relay.cli import read_capture
from cdc_relay.config import load_settings
from cdc_relay.pgoutput import UNCHANGED_TOAST, Commit, LogicalMessage, Update, decode
from cdc_relay.pipeline import Relay
from cdc_relay.routing import Router
from cdc_relay.sinks import RedisStreamSink

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = str(ROOT / "tests" / "fixtures" / "demo_stream.jsonl")


class ListSink:
    name = "list"

    def __init__(self) -> None:
        self.records = []

    def send(self, records) -> None:
        self.records.extend(records)

    def close(self) -> None:
        pass


def settings():
    s = load_settings(str(ROOT / "examples" / "relay.yaml"), env={})
    s.dead_letter_path = None
    return s


def replay(sink) -> Relay:
    relay = Relay(Router(settings().routing), sink)
    for _, data in read_capture(FIXTURE):
        relay.handle(data)
    relay.flush()
    return relay


def test_every_captured_message_decodes() -> None:
    messages = [decode(data) for _, data in read_capture(FIXTURE)]
    assert len(messages) == 51
    assert sum(isinstance(m, Commit) for m in messages) == 12
    toast = [m for m in messages if isinstance(m, Update) and UNCHANGED_TOAST in m.new]
    assert len(toast) == 1
    assert {m.prefix for m in messages if isinstance(m, LogicalMessage)} == {"outbox", "heartbeat"}


def test_rolled_back_booking_never_reaches_the_sink() -> None:
    sink = ListSink()
    replay(sink)
    dumped = json.dumps([r.to_dict() for r in sink.records])
    assert "SC-ROLLED-BACK" not in dumped
    assert len(sink.records) == 20


def test_outbox_events_in_commit_order() -> None:
    sink = ListSink()
    replay(sink)
    events = [
        (r.topic, r.key, r.headers.get("event_type"))
        for r in sink.records
        if r.topic.startswith("swift.events.")
    ]
    assert events == [
        ("swift.events.parcel", "SC-240001", "ParcelBooked"),
        ("swift.events.parcel", "SC-240002", "ParcelBooked"),
        ("swift.events.parcel", "SC-240003", "ParcelBooked"),
        ("swift.events.parcel", "SC-240001", "ParcelStatusChanged"),
        ("swift.events.parcel", "SC-240001", "ParcelStatusChanged"),
        ("swift.events.rider", "1", "RiderAssigned"),
        ("swift.events.parcel", "SC-240003", "ParcelCancelled"),
    ]


def test_table_changes_are_typed_masked_and_filtered() -> None:
    sink = ListSink()
    replay(sink)
    parcels = [r.value for r in sink.records if r.topic == "swift.cdc.parcels"]
    booked = parcels[0]["after"]
    assert booked["cod_amount"] == "1250.00" and booked["tags"] == ["cod"]
    assert booked["meta"]["weight_kg"] > 0
    assert booked["created_at"].endswith("Z")
    assert booked["recipient_phone"].startswith("hmac:")
    assert all("notes" not in (p["after"] or {}) for p in parcels)
    [toast] = [p for p in parcels if p.get("unchanged_toast")]
    assert toast["after"]["status"] == "on_hold" and toast["unchanged_toast"] == ["notes"]
    [deleted] = [p for p in parcels if p["op"] == "delete"]
    assert deleted["before"] == {"id": 3}
    assert not any(r.topic.endswith("sync_scratch") for r in sink.records)
    assert not any(r.topic == "swift.cdc.outbox" for r in sink.records)


def test_ids_and_stream_positions_are_unique_and_increasing() -> None:
    sink = ListSink()
    replay(sink)
    positions = [(r.commit_lsn, r.index) for r in sink.records]
    assert positions == sorted(positions)
    assert len(set(positions)) == len(positions)
    assert len({r.id for r in sink.records}) == 20


def test_non_transactional_message_sorts_before_the_commit_it_shares_an_lsn_with() -> None:
    messages = [decode(data) for _, data in read_capture(FIXTURE)]
    heartbeat = next(m for m in messages if isinstance(m, LogicalMessage) and not m.transactional)
    last_commit = [m for m in messages if isinstance(m, Commit)][-1]
    assert heartbeat.lsn == last_commit.commit_lsn  # what the server really sends
    sink = ListSink()
    relay = replay(sink)
    [record] = [r for r in sink.records if r.topic == "messages.heartbeat"]
    assert record.commit_lsn == heartbeat.lsn - 1
    assert relay.confirmed_lsn == last_commit.end_lsn


def test_replaying_into_redis_twice_is_idempotent() -> None:
    client = fakeredis.FakeRedis()
    first = RedisStreamSink(client)
    replay(first)
    lengths = {k: client.xlen(k) for k in client.keys("*")}
    second = RedisStreamSink(client)
    replay(second)
    assert {k: client.xlen(k) for k in client.keys("*")} == lengths
    assert first.duplicates == 0 and second.duplicates == 20
    assert sum(lengths.values()) == 20
