"""``cdc-relay`` command line."""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import signal
import sys
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from cdc_relay import __version__
from cdc_relay.checkpoint import FileCheckpoint, MemoryCheckpoint
from cdc_relay.config import ConfigError, Settings, build_sink, load_settings
from cdc_relay.lsn import format_lsn, parse_lsn
from cdc_relay.pgoutput import UNCHANGED_TOAST, DecodeError, LogicalMessage, decode
from cdc_relay.pipeline import Relay, RelayError, RelayStopped, RetryPolicy
from cdc_relay.routing import Router
from cdc_relay.sinks import FileSink

log = logging.getLogger("cdc_relay")

LSN_FIELDS = {"lsn", "final_lsn", "commit_lsn", "end_lsn"}


def _jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if value is UNCHANGED_TOAST:
        return "<unchanged toast>"
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


def read_capture(path: str) -> Iterator[tuple[int, bytes]]:
    """Captured streams are JSON lines of ``{"lsn": "0/16B3748", "data": "<hex>"}``."""
    with open(path, encoding="utf-8") as fh:
        for number, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                yield parse_lsn(item["lsn"]), bytes.fromhex(item["data"])
            except (ValueError, KeyError) as exc:
                raise SystemExit(f"{path}:{number}: not a capture line ({exc})") from None


def _relay(settings: Settings, *, checkpoint=None, sink=None) -> Relay:
    return Relay(
        Router(settings.routing),
        sink or build_sink(settings.sink),
        checkpoint=checkpoint,
        dead_letter=FileSink(settings.dead_letter_path) if settings.dead_letter_path else None,
        batch_max_records=settings.batch_max_records,
        batch_max_wait=settings.batch_max_wait,
        retry=RetryPolicy(),
    )


# -- commands --------------------------------------------------------------------------------


def cmd_run(args: argparse.Namespace, settings: Settings) -> int:
    from cdc_relay.server import StatusServer, heartbeat_health
    from cdc_relay.source import ReplicationSource

    checkpoint = (
        FileCheckpoint(settings.checkpoint_path, settings.slot)
        if settings.checkpoint_path
        else MemoryCheckpoint()
    )
    relay = _relay(settings, checkpoint=checkpoint)
    stop = relay.stop
    source = ReplicationSource(settings, relay, stop=stop)

    def on_signal(signum, _frame) -> None:
        log.info("received %s, finishing the current batch", signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    server = None
    if settings.status_port:
        server = StatusServer(
            settings.status_host,
            settings.status_port,
            metrics=relay.metrics,
            status=source.status,
            healthy=heartbeat_health(source, max(30.0, 3 * settings.status_interval)),
        ).start()
        log.info("status server on :%d (/healthz, /metrics, /status)", server.port)
    try:
        source.prepare()
        source.run_forever()
    except (RelayError, RelayStopped) as exc:
        log.error("%s", exc)
        return 1 if isinstance(exc, RelayError) else 0
    finally:
        relay.sink.close()
        if server:
            server.close()
    log.info("stopped at %s", format_lsn(relay.confirmed_lsn))
    return 0


def cmd_slot(args: argparse.Namespace, settings: Settings) -> int:
    import psycopg2

    from cdc_relay import admin

    conn = psycopg2.connect(settings.dsn)
    try:
        if args.action == "create":
            admin.check_server(conn)
            pub = admin.ensure_publication(conn, settings.publication, settings.publication_tables)
            print(f"publication {settings.publication}: {'created' if pub else 'exists'}")
            if settings.snapshot and admin.slot_status(conn, settings.slot) is None:
                # A slot made here would have no snapshot; `run` creates it with one.
                print(
                    f"slot {settings.slot}: left to `cdc-relay run`, which creates it together "
                    "with the initial snapshot"
                )
                return 0
            slot = admin.ensure_slot(conn, settings.slot)
            print(f"slot {settings.slot}: {'created' if slot else 'exists'}")
        elif args.action == "drop":
            dropped = admin.drop_slot(conn, settings.slot)
            print(f"slot {settings.slot}: {'dropped' if dropped else 'not found'}")
            if args.publication:
                admin.drop_publication(conn, settings.publication)
                print(f"publication {settings.publication}: dropped")
        else:
            status = admin.slot_status(conn, settings.slot)
            if status is None:
                print(f"slot {settings.slot} not found", file=sys.stderr)
                return 1
            print(json.dumps(status, indent=2, default=str))
    finally:
        conn.close()
    return 0


def cmd_capture(args: argparse.Namespace, settings: Settings) -> int:
    import psycopg2

    from cdc_relay import admin

    conn = psycopg2.connect(settings.dsn)
    count = 0
    try:
        with open(args.out, "w", encoding="utf-8") as fh:
            for lsn, xid, data in admin.peek_changes(
                conn, settings.slot, settings.publication, args.limit
            ):
                fh.write(json.dumps({"lsn": lsn, "xid": xid, "data": data.hex()}) + "\n")
                count += 1
    finally:
        conn.close()
    print(
        f"captured {count} messages from slot {settings.slot} into {args.out} "
        "(peeked: the slot was not advanced)",
        file=sys.stderr,
    )
    return 0


def cmd_decode(args: argparse.Namespace, settings: Settings) -> int:
    for at, data in read_capture(args.file):
        try:
            message = decode(data)
        except DecodeError as exc:
            print(json.dumps({"at": format_lsn(at), "error": str(exc)}))
            continue
        body = _jsonable(dataclasses.asdict(message))
        for name in LSN_FIELDS & body.keys():
            body[name] = format_lsn(body[name])
        if isinstance(message, LogicalMessage):
            try:
                body["content"] = message.content.decode("utf-8")
            except UnicodeDecodeError:
                pass  # stays hex
        line = {"at": format_lsn(at), "type": type(message).__name__, **body}
        print(json.dumps(line, ensure_ascii=False))
    return 0


def cmd_replay(args: argparse.Namespace, settings: Settings) -> int:
    relay = _relay(settings)
    for _lsn, data in read_capture(args.file):
        relay.handle(data)
    relay.flush()
    relay.sink.close()
    m = relay.metrics
    print(
        f"replayed {int(m.total('cdc_messages_total'))} messages: "
        f"{int(m.total('cdc_transactions_total'))} transactions, "
        f"{int(m.total('cdc_events_total'))} events, "
        f"{int(m.total('cdc_records_delivered_total'))} records delivered, "
        f"{int(m.total('cdc_records_rejected_total'))} rejected",
        file=sys.stderr,
    )
    return 0


def cmd_demo(args: argparse.Namespace, settings: Settings) -> int:
    import psycopg2

    from cdc_relay import demo

    if args.loop:
        demo.run_forever(settings.dsn, args.interval, seed=args.seed)
        return 0
    conn = psycopg2.connect(settings.dsn)
    try:
        result = demo.run_script(conn, seed=args.seed, parcels=args.parcels)
    finally:
        conn.close()
    print(f"demo round wrote parcels {', '.join(result['parcels'])}", file=sys.stderr)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc-relay",
        description="Change data capture for PostgreSQL over native logical replication.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-c", "--config", help="YAML config file (or CDC_CONFIG)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="stream the slot to the configured sink")

    slot = sub.add_parser("slot", help="create, drop or inspect the slot and publication")
    slot.add_argument("action", choices=["create", "drop", "status"])
    slot.add_argument("--publication", action="store_true", help="with drop: also drop it")

    capture = sub.add_parser("capture", help="save pending messages without consuming them")
    capture.add_argument("--out", required=True)
    capture.add_argument("--limit", type=int, default=None, help="stop after N changes")

    dec = sub.add_parser("decode", help="print a captured stream as decoded messages")
    dec.add_argument("file")

    replay = sub.add_parser("replay", help="push a captured stream through routing and the sink")
    replay.add_argument("file")
    replay.add_argument("--sink", choices=["stdout", "file"], help="override sink.type")
    replay.add_argument("--out", help="with --sink file: output path")

    demo = sub.add_parser("demo", help="write demo courier traffic (parcels + outbox)")
    demo.add_argument("--seed", type=int, default=7)
    demo.add_argument("--parcels", type=int, default=3)
    demo.add_argument("--loop", action="store_true", help="repeat every --interval seconds")
    demo.add_argument("--interval", type=float, default=15.0)
    return parser


COMMANDS = {
    "run": cmd_run,
    "slot": cmd_slot,
    "capture": cmd_capture,
    "decode": cmd_decode,
    "replay": cmd_replay,
    "demo": cmd_demo,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = load_settings(args.config)
        if getattr(args, "sink", None):
            settings.sink.type = args.sink
            if args.sink == "file":
                settings.sink.path = args.out or "records.jsonl"
            settings.validate()
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    return COMMANDS[args.command](args, settings)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
