"""Builds pgoutput v1 messages byte by byte, the way a walsender writes them."""

from __future__ import annotations

import struct
from datetime import datetime, timezone

from cdc_relay.pgoutput import PG_EPOCH

TS = datetime(2026, 11, 4, 4, 30, tzinfo=timezone.utc)


def _ts(value: datetime) -> int:
    delta = value - PG_EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _s(text: str) -> bytes:
    return text.encode() + b"\x00"


def tuple_data(values) -> bytes:
    out = struct.pack(">h", len(values))
    for value in values:
        if value is None:
            out += b"n"
        elif value == "<toast>":
            out += b"u"
        elif isinstance(value, bytes):
            out += b"b" + struct.pack(">i", len(value)) + value
        else:
            raw = str(value).encode()
            out += b"t" + struct.pack(">i", len(raw)) + raw
    return out


def begin(final_lsn: int, xid: int, ts: datetime = TS) -> bytes:
    return b"B" + struct.pack(">QqI", final_lsn, _ts(ts), xid)


def commit(commit_lsn: int, end_lsn: int, ts: datetime = TS) -> bytes:
    return b"C" + struct.pack(">BQQq", 0, commit_lsn, end_lsn, _ts(ts))


def origin(lsn: int, name: str) -> bytes:
    return b"O" + struct.pack(">Q", lsn) + _s(name)


def relation(oid: int, namespace: str, name: str, columns, identity: str = "d") -> bytes:
    """``columns`` is a list of ``(name, type_oid, is_key)``."""
    out = b"R" + struct.pack(">I", oid) + _s(namespace) + _s(name) + identity.encode()
    out += struct.pack(">h", len(columns))
    for col_name, type_oid, is_key in columns:
        out += struct.pack(">B", 1 if is_key else 0) + _s(col_name) + struct.pack(">Ii", type_oid, -1)
    return out


def type_msg(oid: int, namespace: str, name: str) -> bytes:
    return b"Y" + struct.pack(">I", oid) + _s(namespace) + _s(name)


def insert(oid: int, values) -> bytes:
    return b"I" + struct.pack(">I", oid) + b"N" + tuple_data(values)


def update(oid: int, values, old=None, old_kind: str = "O") -> bytes:
    out = b"U" + struct.pack(">I", oid)
    if old is not None:
        out += old_kind.encode() + tuple_data(old)
    return out + b"N" + tuple_data(values)


def delete(oid: int, old, old_kind: str = "K") -> bytes:
    return b"D" + struct.pack(">I", oid) + old_kind.encode() + tuple_data(old)


def truncate(oids, cascade: bool = False, restart_identity: bool = False) -> bytes:
    options = (1 if cascade else 0) | (2 if restart_identity else 0)
    return b"T" + struct.pack(">iB", len(oids), options) + b"".join(struct.pack(">I", o) for o in oids)


def message(lsn: int, prefix: str, content: bytes, transactional: bool = True) -> bytes:
    return (
        b"M"
        + struct.pack(">BQ", 1 if transactional else 0, lsn)
        + _s(prefix)
        + struct.pack(">i", len(content))
        + content
    )
