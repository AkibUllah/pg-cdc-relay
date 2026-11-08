import pytest

from cdc_relay.routing import OutboxConfig, Router, RoutingConfig
from tests.factories import event, outbox_row, tx


def route(config: RoutingConfig, *events):
    return Router(config).route(tx(*events))


def test_default_routes_every_table_change_with_the_envelope() -> None:
    [record], rejected = route(RoutingConfig(), event("insert"))
    assert rejected == []
    assert record.topic == "public.parcels"
    assert record.key == '{"id":1}'
    assert record.id == "0/100:0"
    assert record.value["after"] == {"id": 1, "status": "booked"}
    assert record.headers == {"op": "insert"}


def test_include_exclude_globs_and_ops() -> None:
    config = RoutingConfig(include=["public.*"], exclude=["public.audit_*"], ops=["insert"])
    records, _ = route(
        config,
        event("insert", table="parcels"),
        event("insert", table="audit_log"),
        event("update", table="parcels"),
        event("insert", schema="billing", table="invoices"),
    )
    assert [r.topic for r in records] == ["public.parcels"]


def test_drop_and_mask_columns_everywhere_including_the_key() -> None:
    config = RoutingConfig(
        drop_columns={"public.riders": ["nid"]},
        mask_columns={"public.riders": ["phone"]},
        key_columns={"public.riders": ["phone"]},
        mask_salt="s3cret",
    )
    after = {"id": 7, "phone": "+8801711000000", "nid": "1990123456", "name": "Rahim"}
    [record], _ = route(config, event("update", table="riders", after=after, before=dict(after)))
    masked = record.value["after"]["phone"]
    assert masked.startswith("hmac:") and len(masked) == 21
    assert "nid" not in record.value["after"] and "nid" not in record.value["before"]
    assert record.value["before"]["phone"] == masked  # deterministic, so consumers can join
    assert record.value["key"] == {"phone": masked}
    other = Router(RoutingConfig(mask_columns=config.mask_columns, mask_salt="other"))
    [again], _ = other.route(tx(event("insert", table="riders", after=after)))
    assert again.value["after"]["phone"] != masked


def test_mask_needs_a_salt_and_ops_are_validated() -> None:
    with pytest.raises(ValueError, match="mask_salt"):
        RoutingConfig(mask_columns={"public.t": ["x"]})
    with pytest.raises(ValueError, match="unknown ops"):
        RoutingConfig(ops=["upsert"])
    with pytest.raises(ValueError, match="topic_template"):
        RoutingConfig(topic_template="{db}.{table}")


def test_topic_template() -> None:
    [record], _ = route(RoutingConfig(topic_template="swift.cdc.{table}"), event())
    assert record.topic == "swift.cdc.parcels"


def test_truncate_makes_one_record_per_included_table() -> None:
    truncate = event(
        "truncate", table=None, schema=None, tables=["public.parcels", "public.audit_log"]
    )
    records, _ = route(RoutingConfig(exclude=["public.audit_*"]), truncate)
    assert [(r.topic, r.value["tables"]) for r in records] == [
        ("public.parcels", ["public.parcels"])
    ]


def test_outbox_table_insert_publishes_the_payload() -> None:
    config = RoutingConfig(outbox=OutboxConfig(topic_prefix="swift."))
    records, rejected = route(config, event("update"), outbox_row())
    assert rejected == []
    table_change, published = records
    assert table_change.topic == "public.parcels"
    assert published.topic == "swift.parcel"
    assert published.key == "SC-1001"
    assert published.value == {"status": "booked"}
    assert published.headers == {
        "aggregate_type": "parcel",
        "source_lsn": "0/100",
        "event_type": "ParcelBooked",
        "outbox_id": "55",
    }
    assert [r.index for r in records] == [0, 1]


def test_outbox_cleanup_deletes_are_not_published() -> None:
    records, _ = route(RoutingConfig(), event("delete", table="outbox"))
    assert records == []


def test_outbox_table_changes_can_also_be_published() -> None:
    config = RoutingConfig(outbox=OutboxConfig(publish_table_changes=True))
    records, _ = route(config, outbox_row())
    assert [r.topic for r in records] == ["parcel", "public.outbox"]


def test_outbox_message_without_a_table() -> None:
    content = (
        b'{"aggregate_type": "rider", "aggregate_id": 42, "event_type": "RiderAssigned",'
        b' "payload": {"zone": "Mirpur"}}'
    )
    [record], _ = route(RoutingConfig(), event("message", prefix="outbox", content=content))
    assert (record.topic, record.key, record.value) == ("rider", "42", {"zone": "Mirpur"})
    assert record.headers["event_type"] == "RiderAssigned"


@pytest.mark.parametrize(
    ("ev", "reason"),
    [
        (event("message", prefix="outbox", content=b"not json"), "not JSON"),
        (event("message", prefix="outbox", content=b"[1]"), "JSON object"),
        (event("message", prefix="outbox", content=b"\xff"), "JSON object"),
        (outbox_row(aggregate_type=None), "aggregate_type"),
    ],
)
def test_bad_outbox_events_are_rejected_not_dropped(ev, reason: str) -> None:
    records, [rejected] = route(RoutingConfig(), event("insert"), ev)
    assert [r.topic for r in records] == ["public.parcels"]
    assert reason in rejected.reason


def test_plain_text_payload_is_kept() -> None:
    [record], _ = route(RoutingConfig(), outbox_row(payload="hello"))
    assert record.value == "hello"


def test_other_messages_only_when_their_prefix_is_listed() -> None:
    ping = event("message", prefix="heartbeat", content=b"1")
    assert route(RoutingConfig(), ping)[0] == []
    [record], _ = route(RoutingConfig(message_prefixes=["heartbeat"]), ping)
    assert record.topic == "messages.heartbeat"
    assert record.value["content"] == "1"


def test_outbox_can_be_disabled() -> None:
    config = RoutingConfig(outbox=OutboxConfig(table=None, message_prefix=None))
    records, _ = route(config, outbox_row(), event("message", prefix="outbox", content=b"{}"))
    assert [r.topic for r in records] == ["public.outbox"]


# -- snapshot rows (op "read") ---------------------------------------------------------------


def read_event(table: str = "parcels", **kw):
    return event("read", table=table, after={"id": 1, "status": "booked"}, key={"id": 1}, **kw)


def test_read_rows_route_like_inserts_and_carry_the_snapshot_flag() -> None:
    [record], _ = route(RoutingConfig(), read_event())
    assert record.topic == "public.parcels"
    assert record.headers == {"op": "read"}
    assert record.key == '{"id":1}'
    assert record.value["op"] == "read"
    assert record.value["before"] is None
    assert record.value["after"] == {"id": 1, "status": "booked"}
    assert record.value["source"]["snapshot"] is True


def test_read_rows_ignore_the_ops_filter_but_follow_table_filters() -> None:
    config = RoutingConfig(include=["public.*"], exclude=["public.audit_*"], ops=["update"])
    records, _ = route(
        config,
        read_event("parcels"),
        read_event("audit_log"),
        read_event(schema="billing", table="invoices"),
    )
    assert [r.topic for r in records] == ["public.parcels"]


def test_read_rows_are_dropped_and_masked_like_changes() -> None:
    config = RoutingConfig(
        drop_columns={"public.riders": ["nid"]},
        mask_columns={"public.riders": ["phone"]},
        mask_salt="s3cret",
    )
    row = {"id": 7, "phone": "+8801711000000", "nid": "1990123456"}
    [record], _ = route(config, event("read", table="riders", after=row, key={"id": 7}))
    assert "nid" not in record.value["after"]
    assert record.value["after"]["phone"].startswith("hmac:")


def test_outbox_rows_in_a_snapshot_are_not_replayed_as_events() -> None:
    row = outbox_row()
    row.op = "read"
    records, rejected = route(RoutingConfig(), row)
    assert records == [] and rejected == []
    router = Router(RoutingConfig())
    assert router.snapshot_includes("public.outbox") is False
    assert router.snapshot_includes("public.parcels") is True


def test_outbox_rows_in_a_snapshot_with_published_table_changes() -> None:
    row = outbox_row()
    row.op = "read"
    config = RoutingConfig(outbox=OutboxConfig(publish_table_changes=True))
    [record], _ = route(config, row)
    assert record.topic == "public.outbox"
    assert record.headers == {"op": "read"}
    assert "aggregate_type" not in record.headers


def test_snapshot_includes_follows_include_and_exclude() -> None:
    router = Router(RoutingConfig(include=["public.*"], exclude=["public.audit_*"]))
    assert router.snapshot_includes("public.parcels")
    assert not router.snapshot_includes("public.audit_log")
    assert not router.snapshot_includes("billing.invoices")
