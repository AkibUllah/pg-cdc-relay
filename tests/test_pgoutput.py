import pytest

from cdc_relay.pgoutput import (
    UNCHANGED_TOAST,
    Begin,
    Commit,
    DecodeError,
    Delete,
    Insert,
    LogicalMessage,
    Origin,
    Relation,
    Truncate,
    Type,
    Update,
    decode,
)
from tests import pgwire
from tests.pgwire import TS


def test_begin_and_commit() -> None:
    b = decode(pgwire.begin(0x1A2B3C, xid=771))
    assert b == Begin(final_lsn=0x1A2B3C, commit_ts=TS, xid=771)
    c = decode(pgwire.commit(0x1A2B3C, 0x1A2B70))
    assert isinstance(c, Commit)
    assert (c.commit_lsn, c.end_lsn, c.commit_ts) == (0x1A2B3C, 0x1A2B70, TS)


def test_relation_with_keys_and_identity() -> None:
    rel = decode(
        pgwire.relation(
            16390, "public", "parcels", [("id", 20, True), ("status", 25, False)], identity="f"
        )
    )
    assert isinstance(rel, Relation)
    assert rel.qualified_name == "public.parcels"
    assert rel.replica_identity == "full"
    assert [(c.name, c.type_oid, c.is_key) for c in rel.columns] == [
        ("id", 20, True),
        ("status", 25, False),
    ]


def test_relation_in_pg_catalog_has_an_empty_namespace_on_the_wire() -> None:
    rel = decode(pgwire.relation(1, "", "pg_thing", [("a", 23, False)]))
    assert rel.namespace == "pg_catalog"


def test_insert_with_null_text_and_binary_values() -> None:
    msg = decode(pgwire.insert(16390, ["42", None, "ঢাকা", b"\x00\x01"]))
    assert msg == Insert(relation_oid=16390, new=("42", None, "ঢাকা", b"\x00\x01"))


def test_update_without_old_tuple() -> None:
    msg = decode(pgwire.update(16390, ["1", "delivered"]))
    assert msg == Update(relation_oid=16390, new=("1", "delivered"))


@pytest.mark.parametrize("kind", ["K", "O"])
def test_update_with_old_tuple(kind: str) -> None:
    msg = decode(pgwire.update(16390, ["2", "<toast>"], old=["1", "x"], old_kind=kind))
    assert isinstance(msg, Update)
    assert msg.old_kind == kind
    assert msg.old == ("1", "x")
    assert msg.new == ("2", UNCHANGED_TOAST)


def test_delete_key_only() -> None:
    msg = decode(pgwire.delete(16390, ["7", None]))
    assert msg == Delete(relation_oid=16390, old_kind="K", old=("7", None))


def test_truncate_options() -> None:
    msg = decode(pgwire.truncate([1, 2], cascade=True, restart_identity=True))
    assert msg == Truncate(relation_oids=(1, 2), cascade=True, restart_identity=True)
    assert decode(pgwire.truncate([3])).cascade is False


def test_origin_type_and_logical_message() -> None:
    assert decode(pgwire.origin(99, "node_a")) == Origin(lsn=99, name="node_a")
    assert decode(pgwire.type_msg(5000, "public", "mood")) == Type(5000, "public", "mood")
    msg = decode(pgwire.message(123, "outbox", b'{"a":1}', transactional=False))
    assert msg == LogicalMessage(transactional=False, lsn=123, prefix="outbox", content=b'{"a":1}')


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        (b"", "empty"),
        (b"Z", "unknown message type"),
        (b"S\x00\x00\x00\x01\x01", "protocol version 2"),
        (pgwire.begin(1, 2)[:-1], "truncated"),
        (pgwire.begin(1, 2) + b"\x00", "trailing bytes"),
        (b"I\x00\x00\x00\x01X", "marker 'N'"),
        (b"D\x00\x00\x00\x01N", "'K' or 'O'"),
        (b"I\x00\x00\x00\x01N\x00\x01q", "unknown tuple value kind"),
        (b"Y\x00\x00\x00\x01public", "unterminated string"),
    ],
)
def test_malformed_messages_raise_decode_error(payload: bytes, error: str) -> None:
    with pytest.raises(DecodeError, match=error):
        decode(payload)
