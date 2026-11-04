"""Streams pgoutput from a replication slot into the relay, with feedback and reconnects."""

from __future__ import annotations

import logging
import select
import threading
import time
from collections.abc import Callable
from typing import Any

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from cdc_relay import admin
from cdc_relay.config import Settings
from cdc_relay.lsn import format_lsn
from cdc_relay.pipeline import Relay, RelayError, RelayStopped

log = logging.getLogger("cdc_relay.source")


class ReplicationSource:
    def __init__(
        self,
        settings: Settings,
        relay: Relay,
        *,
        stop: threading.Event | None = None,
        connect: Callable[..., Any] = psycopg2.connect,
    ) -> None:
        self.settings = settings
        self.relay = relay
        self.stop = stop or relay.stop
        self.connect = connect
        self.connected = False
        self.heartbeat = time.monotonic()
        self.reconnects = 0
        self.slot: dict[str, Any] | None = None

    # -- setup ---------------------------------------------------------------------------------

    def prepare(self) -> None:
        conn = self.connect(self.settings.dsn)
        try:
            admin.check_server(conn)
            if self.settings.create_slot:
                if admin.ensure_publication(
                    conn, self.settings.publication, self.settings.publication_tables
                ):
                    log.info("created publication %s", self.settings.publication)
                if admin.ensure_slot(conn, self.settings.slot):
                    log.info("created replication slot %s", self.settings.slot)
            self.slot = admin.slot_status(conn, self.settings.slot)
            if self.slot is None:
                raise admin.SetupError(
                    f"slot {self.settings.slot} does not exist (run `cdc-relay slot create`)"
                )
        finally:
            conn.close()

    def poll_lag(self) -> None:
        try:
            conn = self.connect(self.settings.dsn)
            try:
                self.slot = admin.slot_status(conn, self.settings.slot)
            finally:
                conn.close()
        except psycopg2.Error as exc:
            log.warning("slot status query failed: %s", exc)
            return
        if self.slot and self.slot["pending_bytes"] is not None:
            self.relay.metrics.set("cdc_slot_lag_bytes", self.slot["pending_bytes"])

    # -- streaming -----------------------------------------------------------------------------

    def run_forever(self) -> None:
        backoff = 1.0
        while not self.stop.is_set():
            try:
                self.stream()
                return
            except (RelayStopped, RelayError):
                raise
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                self.connected = False
                self.reconnects += 1
                self.relay.reset()
                log.warning("replication connection lost (%s); reconnecting in %.0fs", exc, backoff)
                if self.stop.wait(backoff):
                    return
                backoff = min(backoff * 2, 30.0)

    def stream(self) -> None:
        conn = self.connect(self.settings.dsn, connection_factory=LogicalReplicationConnection)
        try:
            cur = conn.cursor()
            cur.start_replication(
                slot_name=self.settings.slot,
                decode=False,
                start_lsn=self.relay.confirmed_lsn,
                status_interval=self.settings.status_interval,
                options={
                    "proto_version": "1",
                    "publication_names": self.settings.publication,
                    "messages": "true",
                },
            )
            self.connected = True
            self.relay.keepalive = lambda: cur.send_feedback(flush_lsn=self.relay.feedback_lsn)
            log.info(
                "streaming slot %s from %s",
                self.settings.slot,
                format_lsn(self.relay.confirmed_lsn),
            )
            self._loop(cur)
        finally:
            self.connected = False
            self.relay.keepalive = None
            conn.close()

    def _loop(self, cur) -> None:
        relay = self.relay
        reported = relay.feedback_lsn
        next_lag = 0.0
        while not self.stop.is_set():
            self.heartbeat = time.monotonic()
            msg = cur.read_message()
            if msg is not None:
                relay.handle(msg.payload)
            else:
                relay.flush_if_due()
                relay.advance_idle(cur.wal_end)
            if relay.feedback_lsn != reported:
                reported = relay.feedback_lsn
                cur.send_feedback(flush_lsn=reported)
            if self.heartbeat >= next_lag:
                next_lag = self.heartbeat + self.settings.lag_check_interval
                threading.Thread(target=self.poll_lag, daemon=True).start()
            if msg is None:
                timeout = 0.1 if relay.has_pending else 1.0
                select.select([cur], [], [], timeout)
        # Deliver what is complete before exiting so a restart does not resend it.
        relay.flush()
        cur.send_feedback(flush_lsn=relay.feedback_lsn, force=True)

    def status(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "reconnects": self.reconnects,
            "slot": self.slot,
            "relay": self.relay.status(),
        }
