import io
import json

import fakeredis
import httpx
import pytest
import redis

from cdc_relay.routing import Router, RoutingConfig
from cdc_relay.sinks import (
    FileSink,
    PermanentSinkError,
    RedisStreamSink,
    RetryableSinkError,
    StdoutSink,
    WebhookSink,
)
from cdc_relay.sinks.webhook import sign
from tests.factories import event, outbox_row, tx


def records(lsn: int = 0x100):
    out, _ = Router(RoutingConfig()).route(tx(event("insert"), outbox_row(), lsn=lsn))
    return out


def test_stdout_sink_writes_json_lines() -> None:
    buf = io.StringIO()
    StdoutSink(buf).send(records())
    lines = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert [line["topic"] for line in lines] == ["public.parcels", "parcel"]
    assert lines[1]["value"] == {"status": "booked"}


def test_file_sink_appends(tmp_path) -> None:
    path = tmp_path / "out" / "events.jsonl"
    sink = FileSink(str(path))
    sink.send(records(0x100))
    sink.send(records(0x200))
    sink.close()
    ids = [json.loads(line)["id"] for line in path.read_text().splitlines()]
    assert ids == ["0/100:0", "0/100:1", "0/200:0", "0/200:1"]


class Recorder:
    def __init__(self, *responses: httpx.Response | Exception) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        item = self.responses.pop(0) if self.responses else httpx.Response(200)
        if isinstance(item, Exception):
            raise item
        return item


def webhook(handler: Recorder, **kwargs) -> WebhookSink:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return WebhookSink(
        "https://hooks.example/cdc", client=client, clock=lambda: 1762230600, **kwargs
    )


def test_webhook_signs_the_body_and_sets_an_idempotency_key() -> None:
    handler = Recorder()
    webhook(handler, secret="whsec").send(records())
    [request] = handler.requests
    body = request.content
    assert json.loads(body)["records"][0]["id"] == "0/100:0"
    assert request.headers["Idempotency-Key"] == "0/100:0..0/100:1"
    assert request.headers["X-CDC-Timestamp"] == "1762230600"
    assert request.headers["X-CDC-Signature"] == sign("whsec", "1762230600", body)


def test_webhook_without_secret_sends_no_signature() -> None:
    handler = Recorder()
    webhook(handler).send(records())
    assert "X-CDC-Signature" not in handler.requests[0].headers


def test_webhook_splits_large_batches() -> None:
    handler = Recorder()
    webhook(handler, max_batch=1).send(records())
    assert [r.headers["Idempotency-Key"] for r in handler.requests] == [
        "0/100:0..0/100:0",
        "0/100:1..0/100:1",
    ]


@pytest.mark.parametrize("status", [408, 409, 429, 500, 503])
def test_webhook_retryable_statuses(status: int) -> None:
    handler = Recorder(httpx.Response(status, headers={"Retry-After": "7"}))
    with pytest.raises(RetryableSinkError) as info:
        webhook(handler).send(records())
    assert info.value.retry_after == 7.0


@pytest.mark.parametrize("status", [400, 401, 404, 422])
def test_webhook_permanent_statuses(status: int) -> None:
    with pytest.raises(PermanentSinkError, match=f"HTTP {status}"):
        webhook(Recorder(httpx.Response(status, text="nope"))).send(records())


def test_webhook_transport_errors_are_retryable() -> None:
    handler = Recorder(httpx.ConnectError("refused"))
    with pytest.raises(RetryableSinkError, match="ConnectError"):
        webhook(handler).send(records())


def test_webhook_rejects_bad_batch_size() -> None:
    with pytest.raises(ValueError):
        WebhookSink("https://x", max_batch=0)


def test_redis_streams_use_lsn_ids_and_ignore_replays() -> None:
    client = fakeredis.FakeRedis()
    sink = RedisStreamSink(client, stream_template="cdc:{topic}")
    sink.send(records(0x100))
    sink.send(records(0x200))
    sink.send(records(0x100))  # redelivered after a crash
    assert sink.duplicates == 2
    entries = client.xrange("cdc:public.parcels")
    assert [entry_id for entry_id, _ in entries] == [b"256-0", b"512-0"]
    fields = entries[0][1]
    assert fields[b"id"] == b"0/100:0"
    assert json.loads(fields[b"value"])["after"]["status"] == "booked"
    [(outbox_id, outbox_fields)] = client.xrange("cdc:parcel", count=1)
    assert outbox_id == b"256-1" and outbox_fields[b"key"] == b"SC-1001"


def test_redis_connection_errors_are_retryable() -> None:
    class DownPipeline:
        def xadd(self, *args, **kwargs) -> None:
            pass

        def execute(self, raise_on_error: bool):
            raise redis.ConnectionError("Connection refused")

    class Down:
        def pipeline(self, transaction: bool) -> DownPipeline:
            return DownPipeline()

    with pytest.raises(RetryableSinkError):
        RedisStreamSink(Down()).send(records())


def test_redis_other_errors_are_permanent() -> None:
    client = fakeredis.FakeRedis()
    client.set("cdc:public.parcels", "not a stream")
    with pytest.raises(PermanentSinkError, match="WRONGTYPE"):
        RedisStreamSink(client, stream_template="cdc:{topic}").send(records())
