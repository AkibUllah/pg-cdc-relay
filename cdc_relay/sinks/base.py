"""Sink contract.

``send`` delivers a batch of records or raises. The relay confirms the replication slot only
after ``send`` returns, so a sink must not return before its destination has accepted the
records. Delivery is at least once: after a crash or a retried batch a consumer can see a
record twice, and should deduplicate on ``record.id``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from cdc_relay.routing import Record


class SinkError(Exception):
    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class RetryableSinkError(SinkError):
    """Transient: the relay backs off and sends the same batch again."""


class PermanentSinkError(SinkError):
    """Retrying will not help (bad request, auth). Goes to the dead letter file or stops."""


class Sink(Protocol):
    name: str

    def send(self, records: Sequence[Record]) -> None: ...

    def close(self) -> None: ...
