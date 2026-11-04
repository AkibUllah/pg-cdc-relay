"""Append records to Redis Streams, one stream per topic.

Entry ids are derived from the source position, ``<commit_lsn>-<index>``. Commit LSNs only grow
in the order the server streams transactions, so ids are strictly increasing per stream, and
a batch redelivered after a crash is rejected by Redis itself ("equal or smaller than the
target stream top item"). The sink counts those as duplicates instead of failing: replays are
idempotent without any extra bookkeeping.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from cdc_relay.routing import Record
from cdc_relay.sinks.base import PermanentSinkError, RetryableSinkError
from cdc_relay.sinks.jsonl import dumps

_DUPLICATE = "equal or smaller than the target stream top item"


def entry_id(record: Record) -> str:
    return f"{record.commit_lsn}-{record.index}"


class RedisStreamSink:
    name = "redis"

    def __init__(
        self,
        client: Any,
        *,
        stream_template: str = "{topic}",
        maxlen: int | None = None,
    ) -> None:
        self.client = client
        self.stream_template = stream_template
        self.maxlen = maxlen
        self.duplicates = 0

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> RedisStreamSink:
        import redis

        return cls(redis.Redis.from_url(url, socket_timeout=10), **kwargs)

    def send(self, records: Sequence[Record]) -> None:
        import redis

        if not records:
            return
        pipe = self.client.pipeline(transaction=False)
        for record in records:
            fields = {
                "id": record.id,
                "key": record.key or "",
                "headers": dumps(record.headers),
                "value": dumps(record.value),
            }
            pipe.xadd(
                self.stream_template.format(topic=record.topic),
                fields,
                id=entry_id(record),
                maxlen=self.maxlen,
                approximate=True,
            )
        try:
            results = pipe.execute(raise_on_error=False)
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            raise RetryableSinkError(f"redis: {exc}") from exc
        for record, result in zip(records, results, strict=True):
            if not isinstance(result, Exception):
                continue
            if _DUPLICATE in str(result):
                self.duplicates += 1
                continue
            if isinstance(result, (redis.ConnectionError, redis.TimeoutError)):
                raise RetryableSinkError(f"redis: {result}")
            raise PermanentSinkError(f"redis XADD {record.topic} {entry_id(record)}: {result}")

    def close(self) -> None:
        self.client.close()
