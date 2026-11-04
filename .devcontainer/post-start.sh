#!/usr/bin/env bash
# Codespaces start hook: Postgres (wal_level=logical) + Redis + relay + demo writer.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --build

echo "waiting for the relay on :8086 ..."
for _ in $(seq 1 90); do
  if curl -fsS http://127.0.0.1:8086/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

cat <<'MSG'

pg-cdc-relay is streaming the demo courier database into Redis Streams.
  Relay status:   forwarded port 8086, /status and /metrics
  Outbox events:  docker compose exec redis redis-cli XRANGE swift.events.parcel - +
  Table changes:  docker compose exec redis redis-cli XREVRANGE swift.cdc.parcels + - COUNT 3
  Write by hand:  docker compose exec postgres psql -U courier -d courier
  Unit tests:     make test
MSG
