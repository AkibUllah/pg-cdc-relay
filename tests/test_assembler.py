import pytest

from cdc_relay.assembler import StreamError, TransactionAssembler
from cdc_relay.pgoutput import decode
from tests import pgwire
from tests.pgwire import TS

PARCELS = pgwire.relation(
    16390,
    "public",
    "parcels",
    [("id", 20, True), ("status", 25, False), ("cod_amount", 1700, False), ("notes", 25, False)],
)


def feed(asm: TransactionAssembler, *payloads: bytes):
    out = []
    for payload in payloads:
        out.extend(asm.feed(decode(payload)))
    return out


def test_insert_update_delete_in_one_transaction() -> None:
    asm = TransactionAssembler()
    [tx] = feed(
        asm,
        pgwire.begin(0x100, 900),
        PARCELS,
        pgwire.insert(16390, ["1", "booked", "1250.50", None]),
        pgwire.update(16390, ["1", "picked_up", "1250.50", None]),
        pgwire.delete(16390, ["1", None, None, None]),
        pgwire.commit(0x100, 0x180),
    )
    assert (tx.xid, tx.commit_lsn, tx.end_lsn, tx.commit_ts) == (900, 0x100, 0x180, TS)
    insert, update, delete = tx.events
    assert insert.op == "insert"
    assert insert.after == {"id": 1, "status": "booked", "cod_amount": "1250.50", "notes": None}
    assert insert.key == {"id": 1}
    assert update.before is None and update.after["status"] == "picked_up"
    assert delete.before == {"id": 1} and delete.after is None and delete.key == {"id": 1}
    assert [e.id for e in tx.events] == ["0/100:0", "0/100:1", "0/100:2"]
    assert not asm.in_transaction


def test_key_change_reports_the_old_key() -> None:
    asm = TransactionAssembler()
    [tx] = feed(
        asm,
        PARCELS,
        pgwire.begin(0x200, 901),
        pgwire.update(16390, ["2", "booked", "0", None], old=["1", None, None, None], old_kind="K"),
        pgwire.commit(0x200, 0x240),
    )
    [event] = tx.events
    assert event.before == {"id": 1}
    assert event.key == {"id": 2}


def test_unchanged_toast_is_listed_without_a_full_old_row() -> None:
    asm = TransactionAssembler()
    [tx] = feed(
        asm,
        PARCELS,
        pgwire.begin(0x300, 902),
        pgwire.update(16390, ["1", "delivered", "0", "<toast>"]),
        pgwire.commit(0x300, 0x340),
    )
    [event] = tx.events
    assert "notes" not in event.after
    assert event.unchanged_toast == ["notes"]
    assert event.to_dict()["unchanged_toast"] == ["notes"]


def test_unchanged_toast_is_filled_from_replica_identity_full() -> None:
    asm = TransactionAssembler()
    [tx] = feed(
        asm,
        PARCELS,
        pgwire.begin(0x300, 902),
        pgwire.update(
            16390,
            ["1", "delivered", "0", "<toast>"],
            old=["1", "booked", "0", "fragile, call first"],
        ),
        pgwire.commit(0x300, 0x340),
    )
    [event] = tx.events
    assert event.after["notes"] == "fragile, call first"
    assert event.before["status"] == "booked"
    assert event.unchanged_toast == []


def test_truncate_and_transactional_message() -> None:
    asm = TransactionAssembler()
    [tx] = feed(
        asm,
        PARCELS,
        pgwire.relation(16400, "public", "scratch", [("id", 23, True)]),
        pgwire.begin(0x400, 903),
        pgwire.origin(0x3FF, "branch_ctg"),
        pgwire.truncate([16390, 16400], cascade=True),
        pgwire.message(0x3F0, "outbox", b'{"x": 1}'),
        pgwire.commit(0x400, 0x440),
    )
    truncate, message = tx.events
    assert truncate.tables == ["public.parcels", "public.scratch"] and truncate.cascade
    assert message.prefix == "outbox" and message.content == b'{"x": 1}'
    assert tx.origin == "branch_ctg" and message.origin == "branch_ctg"
    assert message.to_dict()["content"] == '{"x": 1}'


def test_non_transactional_message_is_its_own_transaction() -> None:
    asm = TransactionAssembler()
    txs = asm.feed(decode(pgwire.message(0x500, "audit", b"\xff\xfe", transactional=False)), 0x520)
    [tx] = txs
    assert tx.xid is None and tx.end_lsn == 0x520
    assert tx.events[0].to_dict()["content_hex"] == "fffe"


def test_relation_cache_follows_schema_changes() -> None:
    asm = TransactionAssembler()
    [tx] = feed(
        asm,
        PARCELS,
        pgwire.begin(0x600, 904),
        pgwire.relation(16390, "public", "parcels", [("id", 20, True), ("weight_kg", 1700, False)]),
        pgwire.insert(16390, ["9", "2.5"]),
        pgwire.commit(0x600, 0x640),
    )
    assert tx.events[0].after == {"id": 9, "weight_kg": "2.5"}


def test_binary_values_become_hex() -> None:
    asm = TransactionAssembler()
    [tx] = feed(
        asm,
        pgwire.relation(1, "public", "blobs", [("id", 23, True), ("data", 17, False)]),
        pgwire.begin(0x700, 905),
        pgwire.insert(1, ["1", b"\x01\xab"]),
        pgwire.commit(0x700, 0x740),
    )
    assert tx.events[0].after["data"] == "01ab"


@pytest.mark.parametrize(
    ("payloads", "error"),
    [
        ([pgwire.insert(16390, ["1", "a", "0", None])], "outside a transaction"),
        ([pgwire.begin(1, 1), pgwire.begin(2, 2)], "inside xid"),
        ([pgwire.begin(1, 1), pgwire.insert(77, ["1"])], "before its Relation"),
        ([PARCELS, pgwire.begin(1, 1), pgwire.insert(16390, ["1"])], "tuple has 1 values"),
        ([pgwire.begin(1, 1), pgwire.commit(2, 3)], "does not match"),
    ],
)
def test_stream_errors(payloads, error: str) -> None:
    with pytest.raises(StreamError, match=error):
        feed(TransactionAssembler(), *payloads)


def test_event_envelope() -> None:
    asm = TransactionAssembler()
    [tx] = feed(
        asm,
        PARCELS,
        pgwire.begin(0x1_0000_0100, 906),
        pgwire.insert(16390, ["1", "booked", "10", None]),
        pgwire.commit(0x1_0000_0100, 0x1_0000_0140),
    )
    assert tx.events[0].to_dict() == {
        "id": "1/100:0",
        "op": "insert",
        "key": {"id": 1},
        "before": None,
        "after": {"id": 1, "status": "booked", "cod_amount": "10", "notes": None},
        "source": {
            "lsn": "1/100",
            "xid": 906,
            "commit_ts": "2026-11-04T04:30:00Z",
            "schema": "public",
            "table": "parcels",
        },
    }
