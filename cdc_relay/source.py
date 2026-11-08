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

from cdc_relay import admin, snapshot
from cdc_relay.checkpoint import SNAPSHOT_DONE, SNAPSHOT_PENDING
from cdc_relay.config import Settings
from cdc_relay.lsn import format_lsn, parse_lsn
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
        self.snapshotting = False
        self.snapshot: dict[str, Any] | None = None

    # -- setup ---------------------------------------------------------------------------------

    def prepare(self) -> None:
        conn = self.connect(self.settings.dsn)
        try:
            server = admin.check_server(conn)
            if self.settings.create_slot:
                if admin.ensure_publication(
                    conn, self.settings.publication, self.settings.publication_tables
                ):
                    log.info("created publication %s", self.settings.publication)
                if self.settings.snapshot:
                    self._prepare_snapshot(conn, server["server_version_num"])
                elif admin.ensure_slot(conn, self.settings.slot):
                    log.info("created replication slot %s", self.settings.slot)
            self.slot = admin.slot_status(conn, self.settings.slot)
            if self.slot is None:
                raise admin.SetupError(
                    f"slot {self.settings.slot} does not exist (run `cdc-relay slot create`)"
                )
        finally:
            conn.close()

    # -- initial snapshot ------------------------------------------------------------------------

    def _prepare_snapshot(self, conn, server_version: int) -> None:
        """Create the slot together with a snapshot of the existing rows, once.

        The snapshot only exists while the connection that created the slot is open, so it is
        taken when the relay creates the slot, and a snapshot that did not finish (the
        checkpoint still says pending) is redone from scratch with a new slot.
        """
        slot = self.settings.slot
        checkpoint = self.relay.checkpoint
        state = checkpoint.snapshot_state()
        exists = admin.slot_status(conn, slot) is not None
        if exists and state == SNAPSHOT_PENDING:
            log.warning("the initial snapshot for slot %s did not finish; starting over", slot)
            admin.drop_slot(conn, slot)
            exists = False
        if exists:
            if state != SNAPSHOT_DONE:
                log.info(
                    "slot %s already exists; the initial snapshot only runs with a new slot", slot
                )
            return
        checkpoint.save_snapshot(SNAPSHOT_PENDING)
        try:
            self.take_snapshot(server_version)
        except BaseException:
            # Without the slot the next start takes the snapshot again instead of streaming
            # from a position whose earlier rows were never delivered.
            self._drop_slot_quietly()
            raise

    def take_snapshot(self, server_version: int) -> dict[str, Any]:
        s = self.settings
        self.snapshotting = True
        self.heartbeat = time.monotonic()
        replication = self.connect(s.dsn, connection_factory=LogicalReplicationConnection)
        try:
            cur = replication.cursor()
            # The replication protocol's CREATE_REPLICATION_SLOT exports the snapshot the slot
            # starts from; the SQL function cannot.
            cur.create_replication_slot(s.slot, output_plugin="pgoutput")
            _, point, snapshot_name, _ = cur.fetchone()
            consistent_point = parse_lsn(point)
            log.info(
                "created replication slot %s at %s; reading the initial snapshot",
                s.slot,
                point,
            )
            self.snapshot = {"state": "running", "consistent_point": point}
            reader = self.connect(s.dsn)
            try:
                snapshot.begin(reader, snapshot_name)
                with reader.cursor() as meta:
                    tables = snapshot.published_tables(meta, s.publication, server_version)
                snapshot.read_as_text(reader, tables)
                snap = snapshot.Snapshot(
                    self.relay.router,
                    self.relay.deliver,
                    consistent_point=consistent_point,
                    batch_size=s.batch_max_records,
                    on_rows=self._snapshot_progress,
                )
                result = snap.read(reader, tables)
                reader.rollback()
            finally:
                reader.close()
        finally:
            replication.close()
            self.snapshotting = False
        self.relay.checkpoint.save_snapshot(SNAPSHOT_DONE, consistent_point)
        self.relay.start_at(consistent_point)
        self.snapshot = {"state": "done", **result.to_dict()}
        self.relay.metrics.set("cdc_snapshot_completed", 1)
        log.info(
            "initial snapshot done: %d rows from %d tables, %d records",
            result.rows,
            len(result.tables),
            result.records,
        )
        return self.snapshot

    def _snapshot_progress(self, table: str, rows: int) -> None:
        self.heartbeat = time.monotonic()
        self.relay.metrics.inc("cdc_snapshot_rows_total", rows, table=table)
        if self.stop.is_set():
            raise RelayStopped("stopped during the initial snapshot")

    def _drop_slot_quietly(self) -> None:
        try:
            conn = self.connect(self.settings.dsn)
            try:
                admin.drop_slot(conn, self.settings.slot)
            finally:
                conn.close()
        except psycopg2.Error as exc:
            log.warning(
                "could not drop slot %s after a failed snapshot: %s", self.settings.slot, exc
            )

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
            "snapshot": self.snapshot,
            "relay": self.relay.status(),
        }
