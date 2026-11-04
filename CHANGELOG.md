# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-04

### Added

- **pgoutput decoding:**
  - a pgoutput protocol v1 decoder for every message type, including logical messages
  - text-format value conversion by type OID
- **Change events:**
  - transaction assembly with a relation cache, replica-identity keys and unchanged-TOAST tracking
  - non-transactional logical messages are positioned so their ids never collide with the next commit
- **Routing:**
  - include/exclude globs, op filters, dropped and HMAC-masked columns, key overrides and a topic template
  - the transactional outbox through a table insert or `pg_logical_emit_message`
- **Sinks:** stdout and file (JSON lines), webhooks (HMAC signature, idempotency key, Retry-After), and Redis Streams with LSN-derived entry ids that make replays idempotent.
- **At-least-once delivery:**
  - whole-transaction batches, confirmed only after the sink accepts them
  - a local checkpoint against redelivery after a crash
  - a dead letter file for permanent failures
  - reconnects without duplicates
  - keepalives during sink outages
  - slot advancement on idle and filtered streams
- **Operations:**
  - `/healthz`, Prometheus `/metrics` and `/status`
  - slot lag metric
  - graceful drain on SIGTERM
- **CLI:** `cdc-relay run`, `slot create|drop|status`, `capture` (peek without consuming), `decode`, `replay`, `demo`.
- **Tooling:**
  - a Docker image and a compose stack (PostgreSQL 16, Redis, relay, demo writer)
  - CI with end-to-end tests on PostgreSQL 14, 16 and 17
  - a GitHub Pages walkthrough built from a captured stream
  - Codespaces

[0.1.0]: https://github.com/AkibUllah/pg-cdc-relay/releases/tag/v0.1.0
