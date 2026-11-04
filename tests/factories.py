"""Hand-built transactions for routing, sink and pipeline tests."""

from __future__ import annotations

from cdc_relay.events import ChangeEvent, Transaction
from tests.pgwire import TS


def event(op: str = "insert", table: str = "parcels", seq: int = 0, lsn: int = 0x100, **kw):
    defaults = {"schema": "public", "table": table, "xid": 900, "commit_ts": TS}
    if op in ("insert", "update"):
        defaults["after"] = {"id": 1, "status": "booked"}
        defaults["key"] = {"id": 1}
    if op == "delete":
        defaults["before"] = {"id": 1}
        defaults["key"] = {"id": 1}
    defaults.update(kw)
    return ChangeEvent(op=op, commit_lsn=lsn, seq=seq, **defaults)


def tx(*events: ChangeEvent, lsn: int = 0x100, end: int | None = None) -> Transaction:
    for i, e in enumerate(events):
        e.seq, e.commit_lsn = i, lsn
    return Transaction(
        commit_lsn=lsn,
        end_lsn=end if end is not None else lsn + 0x40,
        xid=900,
        commit_ts=TS,
        events=list(events),
    )


def outbox_row(aggregate_type="parcel", aggregate_id="SC-1001", payload='{"status": "booked"}'):
    return event(
        "insert",
        table="outbox",
        after={
            "id": 55,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": "ParcelBooked",
            "payload": payload,
        },
        key={"id": 55},
    )
