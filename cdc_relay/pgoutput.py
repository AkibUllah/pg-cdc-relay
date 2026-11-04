"""Decoder for the ``pgoutput`` logical replication protocol, version 1.

Each XLogData payload a walsender streams carries exactly one message, identified by its first
byte. The formats are documented in "Logical Replication Message Formats" of the PostgreSQL
manual. Integers are big-endian, strings are NUL-terminated, and tuple values arrive in text
format unless the subscriber asked for binary.

Only protocol version 1 is decoded: the relay never asks for streaming of in-progress
transactions (version 2+), so stream messages are rejected with a clear error instead of being
misread.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

# Replica identity settings carried by Relation messages (pg_class.relreplident).
REPLICA_IDENTITY = {"d": "default", "n": "nothing", "f": "full", "i": "index"}


class DecodeError(ValueError):
    """The payload is not a well-formed pgoutput v1 message."""


class _UnchangedToast:
    """Marker for a TOASTed value the update did not change, so the server did not send it."""

    _instance: _UnchangedToast | None = None

    def __new__(cls) -> _UnchangedToast:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNCHANGED_TOAST"


UNCHANGED_TOAST = _UnchangedToast()

TupleValue = str | bytes | None | _UnchangedToast


def pg_timestamp(micros: int) -> datetime:
    return PG_EPOCH + timedelta(microseconds=micros)


@dataclass(frozen=True)
class Column:
    name: str
    type_oid: int
    type_modifier: int
    is_key: bool


@dataclass(frozen=True)
class Begin:
    final_lsn: int
    commit_ts: datetime
    xid: int


@dataclass(frozen=True)
class Commit:
    flags: int
    commit_lsn: int
    end_lsn: int
    commit_ts: datetime


@dataclass(frozen=True)
class Origin:
    lsn: int
    name: str


@dataclass(frozen=True)
class Relation:
    oid: int
    namespace: str
    name: str
    replica_identity: str
    columns: tuple[Column, ...]

    @property
    def qualified_name(self) -> str:
        return f"{self.namespace}.{self.name}"


@dataclass(frozen=True)
class Type:
    oid: int
    namespace: str
    name: str


@dataclass(frozen=True)
class Insert:
    relation_oid: int
    new: tuple[TupleValue, ...]


@dataclass(frozen=True)
class Update:
    relation_oid: int
    new: tuple[TupleValue, ...]
    # "K": only the replica identity key columns; "O": the whole old row (REPLICA IDENTITY FULL).
    old_kind: str | None = None
    old: tuple[TupleValue, ...] | None = None


@dataclass(frozen=True)
class Delete:
    relation_oid: int
    old_kind: str
    old: tuple[TupleValue, ...]


@dataclass(frozen=True)
class Truncate:
    relation_oids: tuple[int, ...]
    cascade: bool
    restart_identity: bool


@dataclass(frozen=True)
class LogicalMessage:
    """A message written with ``pg_logical_emit_message(transactional, prefix, content)``."""

    transactional: bool
    lsn: int
    prefix: str
    content: bytes = field(repr=False)


Message = (
    Begin | Commit | Origin | Relation | Type | Insert | Update | Delete | Truncate | LogicalMessage
)


class _Reader:
    __slots__ = ("data", "pos")

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def _take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.data):
            raise DecodeError(
                f"message truncated: wanted {n} bytes at offset {self.pos}, "
                f"have {len(self.data) - self.pos}"
            )
        chunk = self.data[self.pos : end]
        self.pos = end
        return chunk

    def byte(self) -> str:
        return chr(self._take(1)[0])

    def int8(self) -> int:
        return self._take(1)[0]

    def int16(self) -> int:
        return struct.unpack(">h", self._take(2))[0]

    def int32(self) -> int:
        return struct.unpack(">i", self._take(4))[0]

    def uint32(self) -> int:
        return struct.unpack(">I", self._take(4))[0]

    def int64(self) -> int:
        return struct.unpack(">q", self._take(8))[0]

    def uint64(self) -> int:
        return struct.unpack(">Q", self._take(8))[0]

    def string(self) -> str:
        end = self.data.find(b"\x00", self.pos)
        if end < 0:
            raise DecodeError(f"unterminated string at offset {self.pos}")
        raw = self.data[self.pos : end]
        self.pos = end + 1
        return raw.decode("utf-8")

    def tuple_data(self) -> tuple[TupleValue, ...]:
        count = self.int16()
        values: list[TupleValue] = []
        for _ in range(count):
            kind = self.byte()
            if kind == "n":
                values.append(None)
            elif kind == "u":
                values.append(UNCHANGED_TOAST)
            elif kind in ("t", "b"):
                length = self.int32()
                raw = self._take(length)
                values.append(raw.decode("utf-8") if kind == "t" else raw)
            else:
                raise DecodeError(f"unknown tuple value kind {kind!r} at offset {self.pos - 1}")
        return tuple(values)

    def finish(self, kind: str) -> None:
        if self.pos != len(self.data):
            raise DecodeError(
                f"{len(self.data) - self.pos} unexpected trailing bytes after {kind!r} message"
            )


def _begin(r: _Reader) -> Begin:
    return Begin(final_lsn=r.uint64(), commit_ts=pg_timestamp(r.int64()), xid=r.uint32())


def _commit(r: _Reader) -> Commit:
    return Commit(
        flags=r.int8(),
        commit_lsn=r.uint64(),
        end_lsn=r.uint64(),
        commit_ts=pg_timestamp(r.int64()),
    )


def _origin(r: _Reader) -> Origin:
    return Origin(lsn=r.uint64(), name=r.string())


def _relation(r: _Reader) -> Relation:
    oid = r.uint32()
    namespace = r.string() or "pg_catalog"  # pgoutput sends "" for pg_catalog
    name = r.string()
    identity = r.byte()
    columns = []
    for _ in range(r.int16()):
        flags = r.int8()
        columns.append(
            Column(
                name=r.string(),
                type_oid=r.uint32(),
                type_modifier=r.int32(),
                is_key=bool(flags & 1),
            )
        )
    return Relation(
        oid=oid,
        namespace=namespace,
        name=name,
        replica_identity=REPLICA_IDENTITY.get(identity, identity),
        columns=tuple(columns),
    )


def _type(r: _Reader) -> Type:
    return Type(oid=r.uint32(), namespace=r.string(), name=r.string())


def _insert(r: _Reader) -> Insert:
    oid = r.uint32()
    marker = r.byte()
    if marker != "N":
        raise DecodeError(f"insert: expected new tuple marker 'N', got {marker!r}")
    return Insert(relation_oid=oid, new=r.tuple_data())


def _update(r: _Reader) -> Update:
    oid = r.uint32()
    marker = r.byte()
    old_kind = old = None
    if marker in ("K", "O"):
        old_kind, old = marker, r.tuple_data()
        marker = r.byte()
    if marker != "N":
        raise DecodeError(f"update: expected new tuple marker 'N', got {marker!r}")
    return Update(relation_oid=oid, new=r.tuple_data(), old_kind=old_kind, old=old)


def _delete(r: _Reader) -> Delete:
    oid = r.uint32()
    marker = r.byte()
    if marker not in ("K", "O"):
        raise DecodeError(f"delete: expected old tuple marker 'K' or 'O', got {marker!r}")
    return Delete(relation_oid=oid, old_kind=marker, old=r.tuple_data())


def _truncate(r: _Reader) -> Truncate:
    count = r.int32()
    options = r.int8()
    oids = tuple(r.uint32() for _ in range(count))
    return Truncate(relation_oids=oids, cascade=bool(options & 1), restart_identity=bool(options & 2))


def _message(r: _Reader) -> LogicalMessage:
    flags = r.int8()
    lsn = r.uint64()
    prefix = r.string()
    length = r.int32()
    return LogicalMessage(
        transactional=bool(flags & 1), lsn=lsn, prefix=prefix, content=r._take(length)
    )


_DECODERS = {
    "B": _begin,
    "C": _commit,
    "O": _origin,
    "R": _relation,
    "Y": _type,
    "I": _insert,
    "U": _update,
    "D": _delete,
    "T": _truncate,
    "M": _message,
}

_STREAMING = {"S": "Stream Start", "E": "Stream Stop", "c": "Stream Commit", "A": "Stream Abort"}


def decode(payload: bytes) -> Message:
    """Decode one pgoutput message."""
    if not payload:
        raise DecodeError("empty message")
    reader = _Reader(bytes(payload))
    kind = reader.byte()
    decoder = _DECODERS.get(kind)
    if decoder is None:
        if kind in _STREAMING:
            raise DecodeError(
                f"{_STREAMING[kind]} message: streaming of in-progress transactions needs "
                "protocol version 2+, which this relay does not request"
            )
        raise DecodeError(f"unknown message type {kind!r}")
    message = decoder(reader)
    reader.finish(kind)
    return message
