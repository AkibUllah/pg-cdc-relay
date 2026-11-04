"""The relay core: decoded messages in, delivered batches out, confirmed position advanced.

It is independent of the replication connection, so it runs the same against a live slot, a
captured stream file, or tests. The source loop calls :meth:`Relay.handle` for every message,
:meth:`Relay.flush_if_due` when the stream is quiet, and sends :attr:`Relay.feedback_lsn` back
to the server.

Ordering and guarantees:

* transactions are delivered in commit order, whole: a batch never ends in the middle of one;
* the confirmed position only moves after the sink accepted every record up to it, so WAL for
  undelivered changes is never released (at-least-once);
* a transaction with no matching records still advances the position, and so does an idle
  stream, so a slot on a busy server does not retain WAL just because no table you follow
  changed.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from cdc_relay.assembler import TransactionAssembler
from cdc_relay.checkpoint import Checkpoint, MemoryCheckpoint
from cdc_relay.events import Transaction
from cdc_relay.lsn import format_lsn
from cdc_relay.metrics import Metrics
from cdc_relay.pgoutput import decode
from cdc_relay.routing import Record, Rejected, Router
from cdc_relay.sinks.base import PermanentSinkError, Sink, SinkError
from cdc_relay.sinks.jsonl import FileSink, dumps

log = logging.getLogger("cdc_relay")


class RelayError(RuntimeError):
    """Delivery cannot continue; the slot keeps the undelivered changes."""


class RelayStopped(RuntimeError):
    """Shutdown was requested while a batch was waiting to be retried."""


@dataclass
class RetryPolicy:
    initial: float = 0.5
    maximum: float = 30.0
    multiplier: float = 2.0
    jitter: float = 0.2
    # None retries forever: a sink outage pauses the relay, the slot keeps the WAL.
    max_attempts: int | None = None

    def delay(self, attempt: int, retry_after: float | None = None) -> float:
        base = min(self.maximum, self.initial * self.multiplier ** (attempt - 1))
        base *= 1 + random.uniform(-self.jitter, self.jitter)
        if retry_after is not None:
            base = max(base, min(retry_after, self.maximum * 10))
        return max(0.0, base)


class Relay:
    def __init__(
        self,
        router: Router,
        sink: Sink,
        *,
        checkpoint: Checkpoint | None = None,
        dead_letter: FileSink | None = None,
        batch_max_records: int = 500,
        batch_max_wait: float = 0.5,
        retry: RetryPolicy | None = None,
        metrics: Metrics | None = None,
        stop: threading.Event | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if batch_max_records < 1:
            raise ValueError("batch_max_records must be at least 1")
        self.router = router
        self.sink = sink
        self.checkpoint = checkpoint or MemoryCheckpoint()
        self.dead_letter = dead_letter
        self.batch_max_records = batch_max_records
        self.batch_max_wait = batch_max_wait
        self.retry = retry or RetryPolicy()
        self.metrics = metrics or Metrics()
        self.stop = stop or threading.Event()
        self.sleep = sleep
        self.clock = clock

        # Called while waiting to retry, so a long sink outage does not trip the server's
        # wal_sender_timeout (the source sends a standby status update).
        self.keepalive: Callable[[], None] | None = None

        self.assembler = TransactionAssembler()
        self.confirmed_lsn = self.checkpoint.load()
        self._idle_lsn = self.confirmed_lsn
        self._records: list[Record] = []
        self._pending_end = 0
        self._pending_txs = 0
        self._pending_since: float | None = None
        self._last_commit_ts: float | None = None
        self.metrics.set("cdc_confirmed_lsn", self.confirmed_lsn)

    # -- input ---------------------------------------------------------------------------------

    def handle(self, payload: bytes) -> None:
        message = decode(payload)
        self.metrics.inc("cdc_messages_total", type=type(message).__name__)
        for tx in self.assembler.feed(message):
            self._accept(tx)
        if len(self._records) >= self.batch_max_records:
            self.flush()

    def _accept(self, tx: Transaction) -> None:
        self.metrics.inc("cdc_transactions_total")
        if tx.end_lsn <= self.confirmed_lsn:
            self.metrics.inc("cdc_transactions_skipped_total")
            log.info("skipping xid %s at %s: already delivered", tx.xid, format_lsn(tx.end_lsn))
            return
        for event in tx.events:
            self.metrics.inc("cdc_events_total", op=event.op)
        records, rejected = self.router.route(tx)
        if rejected:
            self._reject(rejected)
        if self._pending_since is None:
            self._pending_since = self.clock()
        self._records.extend(records)
        self._pending_end = tx.end_lsn
        self._pending_txs += 1
        if tx.commit_ts is not None:
            self._last_commit_ts = tx.commit_ts.timestamp()

    # -- output --------------------------------------------------------------------------------

    @property
    def has_pending(self) -> bool:
        return self._pending_end > self.confirmed_lsn

    @property
    def feedback_lsn(self) -> int:
        """What to report as flushed: delivered position, or the idle server position."""
        return max(self.confirmed_lsn, self._idle_lsn)

    def advance_idle(self, server_lsn: int) -> None:
        """Called when the stream is quiet with everything received so far delivered."""
        if not self.has_pending and not self.assembler.in_transaction:
            self._idle_lsn = max(self._idle_lsn, server_lsn)

    def flush_if_due(self) -> bool:
        if not self.has_pending:
            return False
        waited = self.clock() - (self._pending_since or self.clock())
        if len(self._records) >= self.batch_max_records or waited >= self.batch_max_wait:
            self.flush()
            return True
        return False

    def flush(self) -> int:
        if not self.has_pending:
            return self.confirmed_lsn
        records = self._records
        if records:
            self._deliver(records)
        self.confirmed_lsn = self._pending_end
        self._idle_lsn = max(self._idle_lsn, self.confirmed_lsn)
        self.checkpoint.save(self.confirmed_lsn)
        self.metrics.set("cdc_confirmed_lsn", self.confirmed_lsn)
        if self._last_commit_ts is not None:
            self.metrics.set("cdc_last_commit_timestamp_seconds", self._last_commit_ts)
        log.debug(
            "confirmed %s after %d transactions, %d records",
            format_lsn(self.confirmed_lsn),
            self._pending_txs,
            len(records),
        )
        self._records = []
        self._pending_txs = 0
        self._pending_since = None
        return self.confirmed_lsn

    def _deliver(self, records: list[Record]) -> None:
        started = self.clock()
        attempt = 0
        while True:
            attempt += 1
            try:
                self.sink.send(records)
                break
            except PermanentSinkError as exc:
                if self.dead_letter is None:
                    raise RelayError(
                        f"{self.sink.name} rejected a batch permanently ({exc}); no dead letter "
                        "file is configured, so the relay stops and the slot keeps the changes"
                    ) from exc
                log.error("dead-lettering %d records: %s", len(records), exc)
                self.dead_letter.write_lines(
                    dumps({"error": str(exc), "record": r.to_dict()}) for r in records
                )
                self.metrics.inc("cdc_records_dead_lettered_total", len(records))
                self.metrics.inc("cdc_batch_seconds_sum", self.clock() - started)
                return
            except SinkError as exc:
                if self.retry.max_attempts is not None and attempt >= self.retry.max_attempts:
                    raise RelayError(f"giving up after {attempt} attempts: {exc}") from exc
                delay = self.retry.delay(attempt, exc.retry_after)
                self.metrics.inc("cdc_delivery_retries_total")
                log.warning(
                    "%s failed (attempt %d), retrying in %.1fs: %s",
                    self.sink.name,
                    attempt,
                    delay,
                    exc,
                )
                if self._sleep_or_stop(delay):
                    raise RelayStopped("stopped while retrying delivery") from exc
        self.metrics.inc("cdc_records_delivered_total", len(records))
        self.metrics.inc("cdc_batches_total")
        self.metrics.inc("cdc_batch_seconds_sum", self.clock() - started)

    def _sleep_or_stop(self, delay: float) -> bool:
        if self.sleep is not time.sleep:  # injected clock in tests
            self.sleep(delay)
            if self.keepalive:
                self.keepalive()
            return self.stop.is_set()
        deadline = time.monotonic() + delay
        while (left := deadline - time.monotonic()) > 0:
            if self.stop.wait(min(left, 5.0)):
                return True
            if self.keepalive:
                self.keepalive()
        return self.stop.is_set()

    def _reject(self, rejected: list[Rejected]) -> None:
        self.metrics.inc("cdc_records_rejected_total", len(rejected))
        for item in rejected:
            log.warning("rejected event %s: %s", item.event.id, item.reason)
        if self.dead_letter is not None:
            self.dead_letter.write_lines(
                dumps({"error": item.reason, "event": item.event.to_dict()}) for item in rejected
            )
            self.metrics.inc("cdc_records_dead_lettered_total", len(rejected))

    def reset(self) -> None:
        """Forget everything not yet confirmed, e.g. after the connection dropped.

        The server resends from the slot's confirmed position on reconnect, so pending records
        and a half-received transaction would otherwise be delivered twice.
        """
        self.assembler = TransactionAssembler()
        self._records = []
        self._pending_end = 0
        self._pending_txs = 0
        self._pending_since = None

    def status(self) -> dict:
        return {
            "confirmed_lsn": format_lsn(self.confirmed_lsn),
            "feedback_lsn": format_lsn(self.feedback_lsn),
            "pending_transactions": self._pending_txs,
            "pending_records": len(self._records),
            "in_transaction": self.assembler.in_transaction,
            "relations_cached": len(self.assembler.relations),
        }
