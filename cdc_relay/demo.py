"""Demo workload: a fictional courier ("Swift Courier") writing parcels with an outbox.

Each step is one application transaction, written the way a service would: the state change
and its domain event commit together, either through the ``outbox`` table or through
``pg_logical_emit_message``. One transaction is rolled back on purpose: its event must never
reach a sink.
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS parcels (
    id              bigserial PRIMARY KEY,
    tracking_code   text NOT NULL UNIQUE,
    recipient_name  text NOT NULL,
    recipient_phone text NOT NULL,
    district        text NOT NULL,
    cod_amount      numeric(12, 2) NOT NULL DEFAULT 0,
    status          text NOT NULL DEFAULT 'booked',
    tags            text[] NOT NULL DEFAULT '{}',
    notes           text,
    meta            jsonb NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS riders (
    id    serial PRIMARY KEY,
    name  text NOT NULL,
    phone text NOT NULL,
    zone  text NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id             bigserial PRIMARY KEY,
    aggregate_type text NOT NULL,
    aggregate_id   text NOT NULL,
    event_type     text NOT NULL,
    payload        jsonb NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_scratch (
    id   serial PRIMARY KEY,
    note text
);
"""

DISTRICTS = ["Dhaka", "Chattogram", "Sylhet", "Khulna", "Rajshahi", "Barishal", "Rangpur"]
NAMES = [
    "Nusrat Jahan",
    "Tanvir Hasan",
    "Farhana Akter",
    "Rafiq Uddin",
    "Sadia Islam",
    "Mahmudul Karim",
    "Ayesha Siddiqua",
    "Imran Hossain",
]
RIDERS = [
    ("Kamal Mia", "+8801711000101", "Mirpur"),
    ("Shafiq Ahmed", "+8801811000202", "Uttara"),
    ("Rina Begum", "+8801911000303", "Dhanmondi"),
]
STATUSES = ["picked_up", "in_transit", "out_for_delivery", "delivered"]

log = logging.getLogger("cdc_relay.demo")


def _outbox(cur, aggregate_type: str, aggregate_id: str, event_type: str, payload: dict) -> None:
    cur.execute(
        "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) "
        "VALUES (%s, %s, %s, %s)",
        (aggregate_type, aggregate_id, event_type, json.dumps(payload)),
    )


def setup(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
        cur.execute("SELECT count(*) FROM riders")
        if cur.fetchone()[0] == 0:
            cur.executemany("INSERT INTO riders (name, phone, zone) VALUES (%s, %s, %s)", RIDERS)
    conn.commit()


def book(conn, rng: random.Random) -> str:
    name = rng.choice(NAMES)
    district = rng.choice(DISTRICTS)
    cod = f"{rng.choice([0, 450, 850, 1250, 2300, 5400])}.00"
    fragile = rng.random() < 0.25
    with conn.cursor() as cur:
        # The tracking code comes from the id sequence, so it never repeats after a delete.
        cur.execute(
            "INSERT INTO parcels (id, tracking_code, recipient_name, recipient_phone, district, "
            "cod_amount, tags, meta) VALUES (nextval('parcels_id_seq'), "
            "'SC-' || (240000 + currval('parcels_id_seq')), %s, %s, %s, %s, %s, %s) "
            "RETURNING tracking_code",
            (
                name,
                f"+88017{rng.randrange(10_000_000, 99_999_999)}",
                district,
                cod,
                ["cod"] if cod != "0.00" else ["prepaid"],
                json.dumps({"weight_kg": round(rng.uniform(0.2, 6.0), 1), "fragile": fragile}),
            ),
        )
        code = cur.fetchone()[0]
        _outbox(
            cur,
            "parcel",
            code,
            "ParcelBooked",
            {"tracking_code": code, "district": district, "cod_amount": cod},
        )
    conn.commit()
    return code


def advance(conn, code: str, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE parcels SET status = %s, updated_at = now() WHERE tracking_code = %s",
            (status, code),
        )
        _outbox(
            cur, "parcel", code, "ParcelStatusChanged", {"tracking_code": code, "status": status}
        )
    conn.commit()


def add_long_note(conn, code: str, rng: random.Random) -> None:
    """A note big enough to be stored out of line (TOAST), then an update that leaves it alone.

    Random hex barely compresses, so 12 kB stays above the TOAST threshold.
    """
    note = "Delivery log " + "".join(rng.choice("0123456789abcdef") for _ in range(12_000))
    with conn.cursor() as cur:
        cur.execute("UPDATE parcels SET notes = %s WHERE tracking_code = %s", (note, code))
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE parcels SET status = 'on_hold', updated_at = now() WHERE tracking_code = %s",
            (code,),
        )
    conn.commit()


def assign_rider(conn, code: str, rider_id: int) -> None:
    """Outbox without a table: the event rides in the WAL as a transactional message."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE parcels SET meta = meta || jsonb_build_object('rider_id', %s) "
            "WHERE tracking_code = %s",
            (rider_id, code),
        )
        cur.execute(
            "SELECT pg_logical_emit_message(true, 'outbox', %s)",
            (
                json.dumps(
                    {
                        "aggregate_type": "rider",
                        "aggregate_id": str(rider_id),
                        "event_type": "RiderAssigned",
                        "payload": {"tracking_code": code, "rider_id": rider_id},
                    }
                ),
            ),
        )
    conn.commit()


def failed_booking(conn) -> None:
    """Rolled back: neither the row nor its outbox event may be published."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO parcels (tracking_code, recipient_name, recipient_phone, district) "
            "VALUES ('SC-ROLLED-BACK', 'Nobody', '+8801700000000', 'Dhaka')"
        )
        _outbox(
            cur, "parcel", "SC-ROLLED-BACK", "ParcelBooked", {"tracking_code": "SC-ROLLED-BACK"}
        )
    conn.rollback()


def cancel(conn, code: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM parcels WHERE tracking_code = %s", (code,))
        _outbox(cur, "parcel", code, "ParcelCancelled", {"tracking_code": code})
    conn.commit()


def housekeeping(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO sync_scratch (note) VALUES ('nightly sync')")
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE sync_scratch RESTART IDENTITY")
        cur.execute("DELETE FROM outbox WHERE created_at < now() - interval '7 days'")
        cur.execute("SELECT pg_logical_emit_message(false, 'heartbeat', 'swift-courier')")
    conn.commit()


def run_script(conn, *, seed: int = 7, parcels: int = 3) -> dict[str, Any]:
    """One deterministic round: bookings, status changes, TOAST, rider, rollback, cancel."""
    if parcels < 1:
        raise ValueError("parcels must be at least 1")
    rng = random.Random(seed)
    setup(conn)
    codes = [book(conn, rng) for _ in range(parcels)]
    for status in STATUSES[:2]:
        advance(conn, codes[0], status)
    add_long_note(conn, codes[min(1, len(codes) - 1)], rng)
    assign_rider(conn, codes[0], 1)
    failed_booking(conn)
    cancel(conn, codes[-1])
    housekeeping(conn)
    return {"parcels": codes}


def run_forever(dsn: str, interval: float, seed: int = 7) -> None:  # pragma: no cover - loop
    import psycopg2

    round_no = 0
    while True:
        try:
            conn = psycopg2.connect(dsn)
            try:
                result = run_script(conn, seed=seed + round_no)
                log.info("demo round %d wrote %s", round_no, ", ".join(result["parcels"]))
            finally:
                conn.close()
        except psycopg2.Error as exc:
            log.warning("demo round failed: %s", exc)
        round_no += 1
        time.sleep(interval)
