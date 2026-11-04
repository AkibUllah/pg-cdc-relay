"""Publication and replication slot management over a regular connection."""

from __future__ import annotations

from typing import Any

from psycopg2 import sql


class SetupError(RuntimeError):
    pass


def check_server(conn) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT current_setting('server_version_num')::int, current_setting('wal_level')"
        )
        version, wal_level = cur.fetchone()
    if version < 140000:
        raise SetupError(
            "PostgreSQL 14 or newer is required (pgoutput's messages option, used for the "
            "table-less outbox, arrived in 14)"
        )
    if wal_level != "logical":
        raise SetupError(
            f"wal_level is {wal_level!r}; set wal_level = logical (postgresql.conf or "
            "ALTER SYSTEM) and restart the server"
        )
    return {"server_version_num": version, "wal_level": wal_level}


def _table_identifier(name: str) -> sql.Composable:
    if "." not in name:
        raise SetupError(f"publication table must be schema-qualified: {name!r}")
    schema, table = name.split(".", 1)
    return sql.Identifier(schema, table)


def ensure_publication(conn, name: str, tables: list[str] | None) -> bool:
    """Create the publication if missing. Returns True when it was created."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (name,))
        if cur.fetchone():
            return False
        if tables:
            target = sql.SQL("FOR TABLE {}").format(
                sql.SQL(", ").join(_table_identifier(t) for t in tables)
            )
        else:
            target = sql.SQL("FOR ALL TABLES")
        cur.execute(sql.SQL("CREATE PUBLICATION {} {}").format(sql.Identifier(name), target))
    conn.commit()
    return True


def ensure_slot(conn, slot: str) -> bool:
    """Create the logical slot (pgoutput) if missing. Returns True when it was created."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT plugin, slot_type, database = current_database() "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (slot,),
        )
        row = cur.fetchone()
        if row:
            plugin, slot_type, same_db = row
            if slot_type != "logical" or plugin != "pgoutput":
                raise SetupError(f"slot {slot} exists but is {slot_type}/{plugin}, not pgoutput")
            if not same_db:
                raise SetupError(f"slot {slot} belongs to another database")
            return False
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
    conn.commit()
    return True


def drop_slot(conn, slot: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot,))
        if not cur.fetchone():
            return False
        cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
    conn.commit()
    return True


def drop_publication(conn, name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(name)))
    conn.commit()


def slot_status(conn, slot: str) -> dict[str, Any] | None:
    """Slot position and how much WAL it holds back.

    ``retained_bytes`` is WAL the server must keep for this slot (from ``restart_lsn``);
    ``pending_bytes`` is WAL written since the relay last confirmed (``confirmed_flush_lsn``).
    A stopped relay makes both grow until the disk fills, which is the main operational risk of
    logical replication, so they are exported as metrics.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT active, active_pid, restart_lsn::text, confirmed_flush_lsn::text,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint,
                   wal_status
            FROM pg_replication_slots WHERE slot_name = %s
            """,
            (slot,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    keys = (
        "active",
        "active_pid",
        "restart_lsn",
        "confirmed_flush_lsn",
        "retained_bytes",
        "pending_bytes",
        "wal_status",
    )
    return {"slot": slot, **dict(zip(keys, row, strict=True))}


def peek_changes(conn, slot: str, publication: str, limit: int | None = None):
    """Read pending pgoutput messages without consuming them (for capture and debugging)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT lsn::text, xid::text::bigint, data FROM pg_logical_slot_peek_binary_changes("
            "%s, NULL, %s, 'proto_version', '1', 'publication_names', %s, 'messages', 'true')",
            (slot, limit, publication),
        )
        for lsn, xid, data in cur:
            yield lsn, xid, bytes(data)
