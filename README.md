<!-- topics: postgresql,change-data-capture,cdc,logical-replication,outbox-pattern,pgoutput,redis-streams,webhooks,event-driven,python -->
# pg-cdc-relay

Change data capture for PostgreSQL over native logical replication. It decodes the `pgoutput` protocol itself, publishes transactional outbox events and table changes, and delivers them at least once to webhooks, Redis Streams or files. The replication slot only advances after the sink has the data.

[![CI](https://github.com/AkibUllah/pg-cdc-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/AkibUllah/pg-cdc-relay/actions/workflows/ci.yml)
[![CodeQL](https://github.com/AkibUllah/pg-cdc-relay/actions/workflows/codeql.yml/badge.svg)](https://github.com/AkibUllah/pg-cdc-relay/actions/workflows/codeql.yml)
![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%20%7C%2016%20%7C%2017%20%7C%2018-336791)
![License](https://img.shields.io/badge/license-MIT-green)

## Try it live

| | |
|---|---|
| Live demo | https://akibullah.github.io/pg-cdc-relay/ walks through one round of demo traffic as PostgreSQL 16 streamed it. It shows every pgoutput message as bytes and as decoded fields, then the records the relay delivered. It is built from the same capture the tests replay. |
| Run in your browser | [![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/AkibUllah/pg-cdc-relay?quickstart=1). This starts PostgreSQL with `wal_level=logical`, Redis, the relay and a demo writer. Open forwarded port 8086 for `/status` and `/metrics`, and run `docker compose exec redis redis-cli XRANGE swift.events.parcel - +` to see the events. |
| Local | Run `docker compose up --build` (see Quick Start). |

There is no one-click cloud deploy: the relay needs a PostgreSQL server with logical replication turned on. Most managed services support it behind a flag:
- RDS: `rds.logical_replication = 1`
- Cloud SQL: `cloudsql.logical_decoding = on`
- Azure: `wal_level = logical`

The free tiers of one-click platforms usually do not.

![Walkthrough page: the captured stream, message by message](docs/images/walkthrough.png)

## Why this exists

Services I have worked on, in payments, wallets and order handling, all ran into the same problem: the database changes and something else has to hear about it. That something might be a ledger, a notification service, a search index or a partner's webhook.

Writing to the database and then calling the broker is the dual-write bug. If the process dies between the two, the event is lost. If the transaction rolls back after the call, the event describes something that never happened.

The transactional outbox fixes this by writing the event in the same transaction, but something still has to ship it reliably. Debezium and Kafka Connect do it well, at the price of a JVM and a Kafka cluster. This repo is the small version:
- one Python process reading PostgreSQL's own logical replication stream
- with every step (wire protocol, transaction assembly, delivery and acknowledgement) visible and tested

## Features

- **Its own pgoutput v1 decoder:**
  - Begin, Commit, Relation, Type, Origin, Insert, Update, Delete and Truncate
  - logical messages
  - null, unchanged-TOAST, text and binary values
  - typed errors for truncated, unknown or streaming messages
- **Typed JSON values by type OID:**
  - `numeric` stays a string, so money never rounds
  - `timestamptz` is normalised to UTC
  - `json`/`jsonb` are parsed, `bytea` becomes hex, and one-dimensional arrays become lists
- **Transactions, not rows:**
  - events are grouped by commit
  - keys come from the replica identity
  - the relation cache follows schema changes mid-stream
  - unchanged TOAST columns are listed instead of sent as null
- **Initial snapshot, consistent with the stream:**
  - with `snapshot: true`, the relay creates the slot over the replication protocol and reads the published tables inside the snapshot the slot exports
  - existing rows arrive as `read` records, then streaming starts at the slot's consistent point: no row is missed or read twice, even while the tables are being written
  - snapshot rows use the publication's column lists and row filters (15+) and the replica identity key, and look exactly like streamed inserts
  - a snapshot cut short by a crash is redone with a new slot, never resumed past a gap
- **Transactional outbox, two ways:** an `outbox` table insert, or `pg_logical_emit_message(true, 'outbox', ...)` with no table at all. Payloads are published on `<prefix><aggregate_type>`, keyed by aggregate id.
- **Routing:**
  - include/exclude table globs and op filters
  - per-table dropped columns, and HMAC-masked columns (keys included, so consumers can still join)
  - key overrides for `REPLICA IDENTITY FULL` tables, and a topic template
- **Sinks:**
  - webhooks, with an HMAC-SHA256 signature, an `Idempotency-Key` that is identical on every retry, and `Retry-After`
  - Redis Streams, with LSN-derived entry ids so Redis itself rejects replays
  - stdout or file (JSON lines)
  - a dead letter file for permanent failures
- **At-least-once delivery:**
  - the slot is confirmed only after the sink accepts a batch
  - a local checkpoint skips transactions delivered just before a crash
  - the relay reconnects after a dropped connection without duplicates
  - keepalives continue during sink outages, so the walsender does not time out
- **Slot hygiene:** filtered transactions and idle periods still advance the slot, and slot lag (retained and pending WAL bytes) is exported, so a quiet table never makes PostgreSQL hoard WAL.
- **Operations:**
  - `/healthz`, Prometheus `/metrics` and `/status`
  - graceful drain on SIGTERM
  - a CLI to create and inspect slots, capture a stream without consuming it, decode it, and replay it through routing offline

## Architecture

```mermaid
flowchart LR
    app["Application<br/>UPDATE parcels ...<br/>INSERT INTO outbox ..."] -->|one transaction| pg[(PostgreSQL<br/>wal_level=logical)]
    pg -->|"walsender: pgoutput v1<br/>(committed transactions only)"| src[ReplicationSource<br/>psycopg2 replication conn]
    subgraph relay [cdc-relay]
        src --> dec[pgoutput decoder]
        dec --> asm[Transaction assembler<br/>relation cache, typed values]
        asm --> rt[Router<br/>filters, masking, outbox]
        rt --> batch[Relay<br/>batches whole transactions]
        batch -->|send| sink{{Sink}}
        batch --> cp[(local checkpoint)]
    end
    sink --> wh[Webhook]
    sink --> rs[Redis Streams]
    sink --> fl[JSONL file]
    batch -. "standby status update<br/>flush_lsn = delivered end LSN" .-> pg
    batch --> http["/healthz /metrics /status"]
```

Key decisions:
- **The slot is the source of truth.** PostgreSQL resumes from the slot's `confirmed_flush_lsn`. The relay reports a position only once every record up to it has been accepted by the sink, so a crash, a sink outage or a deploy never loses a change. WAL for undelivered changes simply stays on the server.
- **Whole transactions per batch.** A batch never ends in the middle of a transaction. The confirmed position is always the end of a commit record. Consumers see a transaction's events together and in commit order.
- **Ids are positions.** A record's id is `<commit LSN>:<position>`. It is identical every time the same change is delivered, so consumers can deduplicate without coordination. Redis Streams entry ids use the same numbers, so Redis rejects a replayed batch on its own.
- **Non-transactional messages sit one byte inside their WAL record.** pgoutput reports such a message at the *end* of its record, which can be exactly where the next commit starts. This was found in a real capture, where the two would otherwise get the same id.
- **The snapshot comes from the slot.** `CREATE_REPLICATION_SLOT` over the replication protocol exports the snapshot at the slot's consistent point; a second connection imports it with `SET TRANSACTION SNAPSHOT`. Values are read as the text each type's output function prints, which is what pgoutput streams (a SQL cast is not: `true::text` is `true`, pgoutput sends `t`). Snapshot records use the position just before the consistent point, so their ids and Redis entry ids sort before every streamed change.
- **No Kafka, no JVM.** The decoder is about 340 lines of `struct` reads, and `psycopg2` provides the replication connection. Everything else is standard library plus `httpx`, `pyyaml` and an optional `redis`.

## Delivery guarantees

| Situation | What happens |
|---|---|
| Sink down or slow | The batch is retried with exponential backoff, jitter and `Retry-After`. Nothing is confirmed, and standby status updates keep the replication connection alive. |
| Sink rejects a batch permanently (4xx) | The batch goes to the dead letter file if one is configured, and the relay moves on. Otherwise the relay stops with an error and the slot keeps the changes. |
| Malformed outbox event (not JSON, no `aggregate_type`) | It is rejected with a reason and dead-lettered. The rest of the transaction is delivered. |
| Relay killed after delivery, before feedback | On restart the server resends from the older slot position. The local checkpoint skips those transactions. |
| Relay killed during `send` | The batch is delivered again: at least once. Deduplicate on `id`, or use the Redis sink, which ignores replays. |
| Connection to PostgreSQL lost | Unconfirmed state is discarded and the relay reconnects with backoff. The server resends from the confirmed position, so there are no duplicates. |
| Relay stopped or failing during the initial snapshot | The new slot is dropped and the checkpoint stays `pending`. The next start creates a new slot and reads the tables again. |
| Relay killed hard during the initial snapshot | The slot survives, but the checkpoint still says `pending`, so the next start drops it and takes the snapshot again. Rows already delivered arrive again: deduplicate by `key`. |
| Rolled-back transaction | It is never sent by the server, so neither its rows nor its outbox events reach a sink. |

## Benchmark

`python bench/bench_decode.py`. This is the CPU cost the relay adds per message, on the real capture repeated 2,000 times. It measures one core, with no network and no sink.

```
python 3.10.12 on x86_64, 102,000 messages
| stage                     | messages/s | events/s | records/s |
|---------------------------|-----------:|---------:|----------:|
| decode                    |     82,455 |          |           |
| decode + assemble + route |     22,176 |    9,566 |     8,696 |
```

Routing includes HMAC masking and outbox parsing. In practice the sink round trip dominates; batching (500 records by default) is what keeps throughput up.

## Tech Stack

| Area | Choice |
|---|---|
| Language | Python 3.10+ |
| Replication | PostgreSQL 14+ logical decoding, `pgoutput` protocol v1, `psycopg2` replication connection |
| Sinks | `httpx` (webhooks), Redis Streams (`redis`), JSON lines |
| Config | YAML with environment overrides, strict keys |
| Observability | Prometheus text format, JSON status, structured logs |
| Tests | pytest, fakeredis, a stream captured from PostgreSQL 16, end-to-end tests on 14/16/17/18 |
| Packaging | Docker (non-root), docker compose, GitHub Actions, Codespaces |

## Quick Start

**Docker compose.** This brings up PostgreSQL 16 (logical), Redis, the relay and a demo writer that books parcels every 10 seconds:

```bash
docker compose up --build -d
curl localhost:8086/status
docker compose exec redis redis-cli XRANGE swift.events.parcel - +
docker compose exec redis redis-cli XREVRANGE swift.cdc.parcels + - COUNT 1
docker compose down -v
```

**Local, against your own server.** The server needs `wal_level = logical` and a role with the `REPLICATION` attribute:

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
export CDC_DSN=postgresql://postgres:postgres@localhost:5432/postgres

cdc-relay slot create            # publication + pgoutput slot
cdc-relay demo                   # optional: one round of demo traffic
cdc-relay -c examples/relay.yaml run
```

To start with the rows that are already there, let `run` create the slot with a snapshot (`slot create` then only creates the publication):

```bash
CDC_SLOT=cdc_with_snapshot CDC_SNAPSHOT=true CDC_CHECKPOINT_PATH=state/snapshot.checkpoint.json \
  cdc-relay -c examples/relay.yaml run
```

**No database at all.** Replay the captured stream through routing:

```bash
cdc-relay -c examples/relay.yaml replay tests/fixtures/demo_stream.jsonl --sink stdout
```

## Usage

### Writing outbox events from your application

Write the event in the same transaction as the change it describes:

```sql
BEGIN;
UPDATE parcels SET status = 'delivered' WHERE tracking_code = 'SC-240001';
INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
VALUES ('parcel', 'SC-240001', 'ParcelDelivered', '{"tracking_code": "SC-240001"}');
COMMIT;
```

Or skip the table: the event lives only in the WAL, so there is nothing to clean up:

```sql
SELECT pg_logical_emit_message(true, 'outbox',
  '{"aggregate_type": "rider", "aggregate_id": "42", "event_type": "RiderAssigned",
    "payload": {"zone": "Mirpur"}}');
```

Either way, consumers receive the payload on `swift.events.parcel` / `swift.events.rider`, depending on `outbox.topic_prefix`. The `event_type`, `aggregate_type` and `source_lsn` values come as headers.

### Records

Every sink receives the same record shape:

```json
{
  "id": "0/15FCE60:1",
  "topic": "swift.events.parcel",
  "key": "SC-240001",
  "headers": {"aggregate_type": "parcel", "event_type": "ParcelBooked", "outbox_id": "1", "source_lsn": "0/15FCE60"},
  "value": {"tracking_code": "SC-240001", "district": "Chattogram", "cod_amount": "1250.00"}
}
```

A table change has the full change envelope as its `value`:
- `op`
- `key`
- `before` / `after`
- `unchanged_toast`
- `source`, holding `lsn`, `xid`, `commit_ts`, `schema` and `table`

A row from the initial snapshot has `op` `read`, only `after`, and `"snapshot": true` in `source`. Its `xid` and `commit_ts` are null, because it is not a change. Routing applies to it like to a change (includes, excludes, dropped and masked columns, key overrides), except the `ops` filter. The outbox table is skipped: rows already in it are past events. Set `outbox.publish_table_changes` to receive them as table rows.

### Verifying webhook signatures

```python
import hashlib, hmac, time

def verify(secret: str, body: bytes, timestamp: str, signature: str, tolerance: int = 300) -> bool:
    if abs(time.time() - int(timestamp)) > tolerance:
        return False
    expected = "v1=" + hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

# verify(SECRET, request.body, request.headers["X-CDC-Timestamp"], request.headers["X-CDC-Signature"])
```

Respond 2xx once the batch is stored. Respond 409 or 5xx to have it retried, and deduplicate on `Idempotency-Key` or on each record's `id`.

### Configuration

The full annotated example is in [examples/relay.yaml](examples/relay.yaml). Unknown keys are rejected, so a typo in `mask_columns` cannot publish the column you meant to hide. These environment variables override the file:

| Variable | Setting |
|---|---|
| `CDC_CONFIG` | path of the YAML file |
| `CDC_DSN` (or `DATABASE_URL`) | connection string; `postgresql+driver://` URLs are accepted |
| `CDC_SLOT`, `CDC_PUBLICATION` | slot and publication names |
| `CDC_SNAPSHOT` | `true` to deliver existing rows when the relay creates the slot (default `false`) |
| `CDC_SINK` | `stdout`, `file`, `webhook` or `redis` |
| `CDC_WEBHOOK_URL`, `CDC_WEBHOOK_SECRET` | webhook target and HMAC secret |
| `CDC_REDIS_URL`, `CDC_SINK_PATH` | Redis URL, output file |
| `CDC_MASK_SALT` | secret for masked columns (required when `mask_columns` is set) |
| `CDC_CHECKPOINT_PATH`, `CDC_DEAD_LETTER_PATH` | local state files |
| `CDC_STATUS_PORT` | status server port, `0` to disable (default 8086) |
| `CDC_LOG_LEVEL` | `DEBUG`, `INFO`, ... |

### CLI

| Command | What it does |
|---|---|
| `cdc-relay run` | Streams the slot to the sink, with the status server on :8086. SIGTERM finishes the current batch. |
| `cdc-relay slot create\|drop\|status` | Creates the publication and pgoutput slot (with `snapshot` on, only the publication: `run` creates the slot with its snapshot); drops the slot (`--publication` to drop that too); shows position and retained WAL. |
| `cdc-relay capture --out f.jsonl` | Saves pending messages with `pg_logical_slot_peek_binary_changes`. The slot is not advanced. |
| `cdc-relay decode f.jsonl` | Prints a capture as one decoded message per line. |
| `cdc-relay replay f.jsonl` | Pushes a capture through routing and a sink (`--sink stdout\|file`). Useful for testing routing rules. |
| `cdc-relay demo [--loop]` | Writes demo courier traffic: bookings, outbox, TOAST, a rollback, a cancel and a truncate. |

### Operating notes

- **Watch `cdc_slot_lag_bytes`.** A slot whose consumer is gone retains WAL until the disk fills. Set `max_slot_wal_keep_size` (the compose file uses 1 GB) so PostgreSQL invalidates a slot that falls too far behind instead of going down. Drop the slots you no longer use.
- **Deletes carry only the key** unless the table has `REPLICA IDENTITY FULL`. With FULL, every column is part of the identity: set `routing.key_columns` for that table to keep record keys small.
- **Unchanged TOAST columns** are not sent by the server on updates that did not touch them. They are listed in `unchanged_toast`, so consumers can tell "unchanged" from "set to null".
- **The snapshot runs once per slot.** It only happens when the relay creates the slot, so an existing slot is never read again. To take a new one, stop the relay, drop the slot and delete the checkpoint file. Large tables are read with a server-side cursor in `batch_max_records` chunks; `cdc_snapshot_rows_total{table}` and `/status` show progress, and `/healthz` stays healthy while rows keep coming. The snapshot transaction holds back vacuum on the server until it ends.
- **DDL is not replicated.** After `ALTER TABLE`, the next change brings a new Relation message and the relay picks up the new columns. New tables need to be added to the publication, unless it is `FOR ALL TABLES`.

## Project Structure

```
cdc_relay/
  pgoutput.py       protocol v1 decoder (message dataclasses, tuple values)
  pgtypes.py        text-format value conversion by type OID
  assembler.py      relation cache, transactions of ChangeEvents
  events.py         ChangeEvent / Transaction and the JSON envelope
  routing.py        filters, masking, outbox, topics -> Records
  pipeline.py       Relay: batching, delivery retries, checkpoint, feedback position
  snapshot.py       initial snapshot: published tables, columns, keys, read records
  checkpoint.py     atomic local checkpoint file and snapshot state
  sinks/            stdout/file, webhook, redis_stream
  source.py         replication connection loop, reconnects, slot lag polling
  admin.py          publication/slot management, status, peek
  server.py         /healthz, /metrics, /status
  config.py         YAML + env settings with strict keys
  demo.py           demo courier workload
  cli.py            cdc-relay command
tests/              unit tests, wire encoder, captured-stream replay, end-to-end tests
tests/fixtures/     demo_stream.jsonl captured from PostgreSQL 16
scripts/            Pages walkthrough builder
bench/              decode/route throughput
examples/           relay.yaml (annotated), compose.yaml
```

## Testing

```bash
make test        # 199 tests, no database needed
make pg-up       # throwaway PostgreSQL 16 with wal_level=logical on :55433
make test-pg     # + 10 end-to-end tests against it
make pg-down
```

What is covered:
- **Decoder:** every message type, built byte by byte with a small wire encoder (`tests/pgwire.py`), and the malformed cases.
- **Type conversion:** including time zones, arrays and values that fail to parse.
- **Assembler:** key changes, TOAST with and without a full old row, truncate, origin, schema changes and stream-order errors.
- **Routing:** globs, masking, key overrides, both outbox styles and rejects.
- **Sinks:** signature, idempotency key, status classification, and Redis replay rejection.
- **Pipeline:**
  - batching by size and time
  - idle and filtered slot advancement
  - restart skipping, retries, keepalive during outages, give-up, stop, dead letters and reset
- **Snapshot:** catalog interpretation (replica identity, column lists, generated columns by version), value conversion, ids across batches and tables, skipped tables, slot setup decisions (pending, done, failed, crashed) and health while it runs.
- **Captured stream:** a real PostgreSQL 16 stream is replayed through everything. The tests check that the rolled-back booking never appears, that outbox order matches commits, that positions are unique and increasing, and that a Redis replay is rejected as duplicates.
- **End to end (CI on PostgreSQL 14, 16, 17 and 18):**
  - idempotent slot setup
  - peek without consuming
  - streaming with slot confirmation
  - restart without duplicates
  - reconnect after `pg_terminate_backend` kills the walsender
  - a snapshot taken while a writer updates, inserts and deletes: folding the records gives exactly the table's rows, reads sort before the consistent point and changes after it
  - snapshot values equal to the same row streamed as an insert, across numeric, timestamptz, jsonb, arrays, bytea, uuid and bool
  - an unfinished snapshot redone with a new slot, a finished one never repeated, and row filters and column lists applied

CI also drives the CLI against a real server (capture, decode, replay, and a snapshot of the demo tables), and runs the compose stack until outbox events appear in Redis and the relay resumes from its checkpoint.

## Roadmap

- Protocol v2 streaming of large in-progress transactions, so huge transactions are not buffered whole.
- Kafka and NATS JetStream sinks behind the same `Sink` contract.
- Binary transfer mode for `bytea`-heavy tables.
- Per-topic ordering with parallel delivery for sinks that partition by key.

## License

MIT. See [LICENSE](LICENSE).
