"""POST batches of records to an HTTP endpoint.

The body is ``{"records": [...]}``. Every request carries:

* ``Idempotency-Key``: the first and last record id of the batch, identical on every retry;
* ``X-CDC-Timestamp`` and ``X-CDC-Signature: v1=<hex>``, an HMAC-SHA256 of
  ``"<timestamp>.<body>"`` with the shared secret, when a secret is configured.

2xx is success. 408, 409 (a concurrent duplicate), 425, 429 and 5xx are retried, honouring
``Retry-After``; other 4xx are permanent. Connection errors and timeouts are retried.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Callable, Sequence

import httpx

from cdc_relay.routing import Record
from cdc_relay.sinks.base import PermanentSinkError, RetryableSinkError
from cdc_relay.sinks.jsonl import dumps

RETRYABLE_STATUS = {408, 409, 425, 429}


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return "v1=" + mac.hexdigest()


def _retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


class WebhookSink:
    name = "webhook"

    def __init__(
        self,
        url: str,
        *,
        secret: str | None = None,
        timeout: float = 10.0,
        max_batch: int = 500,
        headers: dict[str, str] | None = None,
        client: httpx.Client | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_batch < 1:
            raise ValueError("max_batch must be at least 1")
        self.url = url
        self.secret = secret
        self.max_batch = max_batch
        self.headers = headers or {}
        self.clock = clock
        self._client = client or httpx.Client(timeout=timeout)

    def send(self, records: Sequence[Record]) -> None:
        for start in range(0, len(records), self.max_batch):
            self._post(records[start : start + self.max_batch])

    def _post(self, batch: Sequence[Record]) -> None:
        if not batch:
            return
        body = dumps({"records": [r.to_dict() for r in batch]}).encode()
        headers = {
            **self.headers,
            "Content-Type": "application/json",
            "Idempotency-Key": f"{batch[0].id}..{batch[-1].id}",
        }
        if self.secret:
            timestamp = str(int(self.clock()))
            headers["X-CDC-Timestamp"] = timestamp
            headers["X-CDC-Signature"] = sign(self.secret, timestamp, body)
        try:
            response = self._client.post(self.url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            raise RetryableSinkError(f"POST {self.url}: {type(exc).__name__}: {exc}") from exc
        status = response.status_code
        if 200 <= status < 300:
            return
        detail = f"POST {self.url}: HTTP {status} {response.text[:200]}"
        if status in RETRYABLE_STATUS or status >= 500:
            raise RetryableSinkError(detail, retry_after=_retry_after(response))
        raise PermanentSinkError(detail)

    def close(self) -> None:
        self._client.close()
