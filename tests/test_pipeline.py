import json

import pytest

from cdc_relay.checkpoint import CheckpointError, FileCheckpoint, MemoryCheckpoint
from cdc_relay.metrics import Metrics
from cdc_relay.pipeline import Relay, RelayError, RelayStopped, RetryPolicy
from cdc_relay.routing import Router, RoutingConfig
from cdc_relay.sinks import FileSink, PermanentSinkError, RetryableSinkError
from tests import pgwire

PARCELS = pgwire.relation(16390, "public", "parcels", [("id", 20, True), ("status", 25, False)])
OTHER = pgwire.relation(16500, "public", "sessions", [("id", 20, True)])


class ListSink:
    name = "list"

    def __init__(self, *failures: Exception) -> None:
        self.failures = list(failures)
        self.batches: list[list] = []
        self.calls = 0

    def send(self, records) -> None:
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        self.batches.append(list(records))

    def close(self) -> None:
        pass

    @property
    def ids(self) -> list[str]:
        return [r.id for batch in self.batches for r in batch]


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def tx_bytes(lsn: int, *changes: bytes) -> list[bytes]:
    return [pgwire.begin(lsn, lsn), *changes, pgwire.commit(lsn, lsn + 0x10)]


def relay(sink=None, **kwargs) -> Relay:
    kwargs.setdefault("sleep", lambda s: None)
    kwargs.setdefault("retry", RetryPolicy(jitter=0))
    return Relay(Router(kwargs.pop("routing", RoutingConfig())), sink or ListSink(), **kwargs)


def feed(r: Relay, payloads) -> None:
    for payload in payloads:
        r.handle(payload)


def test_delivers_on_flush_and_confirms_the_end_of_the_last_transaction() -> None:
    sink = ListSink()
    checkpoint = MemoryCheckpoint()
    r = relay(sink, checkpoint=checkpoint)
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "booked"]))])
    feed(r, tx_bytes(0x200, pgwire.update(16390, ["1", "picked_up"])))
    assert sink.batches == [] and r.has_pending
    assert r.feedback_lsn == 0
    assert r.flush() == 0x210
    assert sink.ids == ["0/100:0", "0/200:0"]
    assert (checkpoint.lsn, r.feedback_lsn) == (0x210, 0x210)
    assert not r.has_pending


def test_batches_flush_by_size_without_splitting_a_transaction() -> None:
    sink = ListSink()
    r = relay(sink, batch_max_records=2)
    feed(r, [PARCELS])
    feed(r, tx_bytes(0x100, *[pgwire.insert(16390, [str(i), "booked"]) for i in range(3)]))
    assert [len(b) for b in sink.batches] == [3]
    assert r.confirmed_lsn == 0x110


def test_flush_if_due_waits_for_batch_max_wait() -> None:
    clock = Clock()
    sink = ListSink()
    r = relay(sink, clock=clock, batch_max_wait=0.5)
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "booked"]))])
    assert r.flush_if_due() is False
    clock.now += 0.6
    assert r.flush_if_due() is True
    assert r.flush_if_due() is False
    assert sink.ids == ["0/100:0"]


def test_filtered_transactions_still_advance_the_slot() -> None:
    sink = ListSink()
    r = relay(sink, routing=RoutingConfig(include=["public.parcels"]))
    feed(r, [OTHER, *tx_bytes(0x100, pgwire.insert(16500, ["1"]))])
    assert r.flush() == 0x110
    assert sink.calls == 0  # nothing to send, but WAL can be released


def test_idle_stream_advances_feedback_but_not_mid_transaction() -> None:
    r = relay()
    r.advance_idle(0x5000)
    assert r.feedback_lsn == 0x5000
    feed(r, [PARCELS, pgwire.begin(0x6000, 1), pgwire.insert(16390, ["1", "x"])])
    r.advance_idle(0x7000)
    assert r.feedback_lsn == 0x5000
    feed(r, [pgwire.commit(0x6000, 0x6010)])
    r.advance_idle(0x8000)  # undelivered transaction pending
    assert r.feedback_lsn == 0x5000
    r.flush()
    r.advance_idle(0x8000)
    assert r.feedback_lsn == 0x8000
    assert r.confirmed_lsn == 0x6010


def test_restart_skips_transactions_the_checkpoint_already_covers(tmp_path) -> None:
    path = str(tmp_path / "relay.checkpoint.json")
    first = ListSink()
    r = relay(first, checkpoint=FileCheckpoint(path, "cdc"))
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    r.flush()
    # crash before feedback: the server resends from its older confirmed position
    second = ListSink()
    metrics = Metrics()
    r2 = relay(second, checkpoint=FileCheckpoint(path, "cdc"), metrics=metrics)
    assert r2.confirmed_lsn == 0x110
    feed(r2, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    feed(r2, tx_bytes(0x200, pgwire.insert(16390, ["2", "b"])))
    r2.flush()
    assert second.ids == ["0/200:0"]
    assert metrics.get("cdc_transactions_skipped_total") == 1


def test_retryable_errors_back_off_and_resend_the_same_batch() -> None:
    delays = []
    sink = ListSink(RetryableSinkError("503"), RetryableSinkError("429", retry_after=5))
    metrics = Metrics()
    r = relay(sink, sleep=delays.append, metrics=metrics)
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    r.flush()
    assert sink.calls == 3 and sink.ids == ["0/100:0"]
    assert delays == [0.5, 5]
    assert metrics.get("cdc_delivery_retries_total") == 2
    assert metrics.get("cdc_records_delivered_total") == 1


def test_keepalive_runs_while_waiting_to_retry() -> None:
    sink = ListSink(RetryableSinkError("down"), RetryableSinkError("down"))
    r = relay(sink)
    pings = []
    r.keepalive = lambda: pings.append(r.feedback_lsn)
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    r.flush()
    assert pings == [0, 0]  # nothing confirmed until the batch is delivered


def test_reset_drops_unconfirmed_state() -> None:
    sink = ListSink()
    r = relay(sink)
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    feed(r, [pgwire.begin(0x200, 2)])
    r.reset()
    assert not r.has_pending and not r.assembler.in_transaction
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])  # resent
    r.flush()
    assert sink.ids == ["0/100:0"]


def test_gives_up_after_max_attempts_without_confirming() -> None:
    sink = ListSink(*[RetryableSinkError("down")] * 3)
    r = relay(sink, retry=RetryPolicy(max_attempts=3, jitter=0))
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    with pytest.raises(RelayError, match="3 attempts"):
        r.flush()
    assert r.confirmed_lsn == 0 and r.has_pending


def test_stop_interrupts_the_retry_loop() -> None:
    r = relay(ListSink(RetryableSinkError("down")))
    r.sleep = lambda s: r.stop.set()
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    with pytest.raises(RelayStopped):
        r.flush()
    assert r.confirmed_lsn == 0


def test_permanent_error_without_dead_letter_stops() -> None:
    r = relay(ListSink(PermanentSinkError("HTTP 400")))
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    with pytest.raises(RelayError, match="no dead letter"):
        r.flush()
    assert r.confirmed_lsn == 0


def test_permanent_error_with_dead_letter_moves_on(tmp_path) -> None:
    path = tmp_path / "deadletter.jsonl"
    r = relay(ListSink(PermanentSinkError("HTTP 422")), dead_letter=FileSink(str(path)))
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    assert r.flush() == 0x110
    [line] = [json.loads(x) for x in path.read_text().splitlines()]
    assert line["error"] == "HTTP 422" and line["record"]["id"] == "0/100:0"
    assert r.metrics.get("cdc_records_dead_lettered_total") == 1


def test_rejected_outbox_events_are_dead_lettered_and_the_rest_delivered(tmp_path) -> None:
    path = tmp_path / "deadletter.jsonl"
    sink = ListSink()
    r = relay(sink, dead_letter=FileSink(str(path)))
    feed(
        r,
        [
            PARCELS,
            *tx_bytes(
                0x100,
                pgwire.insert(16390, ["1", "a"]),
                pgwire.message(0xF0, "outbox", b"{oops"),
            ),
        ],
    )
    r.flush()
    assert sink.ids == ["0/100:0"]
    [line] = [json.loads(x) for x in path.read_text().splitlines()]
    assert "not JSON" in line["error"] and line["event"]["op"] == "message"
    assert r.metrics.get("cdc_records_rejected_total") == 1


def test_metrics_and_status() -> None:
    r = relay()
    feed(r, [PARCELS, *tx_bytes(0x100, pgwire.insert(16390, ["1", "a"]))])
    assert r.status()["pending_transactions"] == 1
    r.flush()
    text = r.metrics.render()
    assert 'cdc_events_total{op="insert"} 1' in text
    assert 'cdc_messages_total{type="Relation"} 1' in text
    assert "cdc_confirmed_lsn 272" in text
    assert "# TYPE cdc_slot_lag_bytes gauge" in text
    assert r.status()["confirmed_lsn"] == "0/110"


def test_retry_delay_grows_caps_and_honours_retry_after() -> None:
    policy = RetryPolicy(initial=1, maximum=8, jitter=0)
    assert [policy.delay(n) for n in range(1, 6)] == [1, 2, 4, 8, 8]
    assert policy.delay(1, retry_after=30) == 30


def test_file_checkpoint_round_trip_and_slot_guard(tmp_path) -> None:
    path = str(tmp_path / "cp" / "relay.checkpoint.json")
    assert FileCheckpoint(path, "a").load() == 0
    FileCheckpoint(path, "a").save(0x1_0000_0010)
    assert FileCheckpoint(path, "a").load() == 0x1_0000_0010
    assert json.loads(open(path).read())["lsn"] == "1/10"
    with pytest.raises(CheckpointError, match="belongs to slot 'a'"):
        FileCheckpoint(path, "b").load()
    open(path, "w").write("{")
    with pytest.raises(CheckpointError, match="unreadable"):
        FileCheckpoint(path, "a").load()


def test_batch_size_is_validated() -> None:
    with pytest.raises(ValueError):
        relay(batch_max_records=0)


def test_file_checkpoint_keeps_the_snapshot_state_across_saves(tmp_path) -> None:
    path = str(tmp_path / "relay.checkpoint.json")
    cp = FileCheckpoint(path, "a")
    assert cp.snapshot_state() is None
    cp.save_snapshot("pending")
    assert FileCheckpoint(path, "a").snapshot_state() == "pending"
    assert FileCheckpoint(path, "a").load() == 0
    cp.save_snapshot("done", 0x16B3748)
    restarted = FileCheckpoint(path, "a")
    assert restarted.load() == 0x16B3748
    restarted.save(0x16B3800)  # delivering batches keeps the state
    data = json.loads(open(path).read())
    assert data["snapshot"] == "done" and data["lsn"] == "0/16B3800"
    assert FileCheckpoint(path, "a").snapshot_state() == "done"


def test_file_checkpoint_save_without_load_keeps_an_existing_state(tmp_path) -> None:
    path = str(tmp_path / "relay.checkpoint.json")
    FileCheckpoint(path, "a").save_snapshot("pending")
    FileCheckpoint(path, "a").save(0x20)
    assert json.loads(open(path).read())["snapshot"] == "pending"


def test_old_checkpoints_without_a_snapshot_field_still_load(tmp_path) -> None:
    path = tmp_path / "relay.checkpoint.json"
    path.write_text('{"slot": "a", "lsn": "0/10", "updated_at": "2026-11-04T00:00:00Z"}')
    cp = FileCheckpoint(str(path), "a")
    assert cp.load() == 0x10 and cp.snapshot_state() is None
    cp.save(0x20)
    assert "snapshot" not in json.loads(path.read_text())


def test_a_checkpoint_that_is_not_an_object_is_rejected(tmp_path) -> None:
    path = tmp_path / "relay.checkpoint.json"
    path.write_text("[1, 2]")
    with pytest.raises(CheckpointError, match="belongs to slot None"):
        FileCheckpoint(str(path), "a").load()


def test_memory_checkpoint_snapshot_state() -> None:
    cp = MemoryCheckpoint()
    assert cp.snapshot_state() is None
    cp.save_snapshot("done", 0x40)
    assert (cp.snapshot_state(), cp.load()) == ("done", 0x40)
