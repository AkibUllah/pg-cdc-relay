"""Throughput of the CPU side of the relay on the captured demo stream.

    python bench/bench_decode.py --repeat 2000

Measures pgoutput decoding alone, then decode + transaction assembly + routing (with masking
and the outbox), on the real 51-message capture repeated N times. No database or sink: this is
the per-message cost the relay adds on top of the network and the sink.
"""

from __future__ import annotations

import argparse
import platform
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from cdc_relay.assembler import TransactionAssembler  # noqa: E402
from cdc_relay.cli import read_capture  # noqa: E402
from cdc_relay.config import load_settings  # noqa: E402
from cdc_relay.pgoutput import decode  # noqa: E402
from cdc_relay.routing import Router  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeat", type=int, default=2000)
    args = parser.parse_args()

    payloads = [data for _, data in read_capture(str(ROOT / "tests/fixtures/demo_stream.jsonl"))]
    stream = payloads * args.repeat
    router = Router(load_settings(str(ROOT / "examples/relay.yaml"), env={}).routing)

    start = time.perf_counter()
    for payload in stream:
        decode(payload)
    decode_s = time.perf_counter() - start

    assembler = TransactionAssembler()
    records = events = 0
    start = time.perf_counter()
    for payload in stream:
        for tx in assembler.feed(decode(payload)):
            events += len(tx.events)
            records += len(router.route(tx)[0])
    full_s = time.perf_counter() - start

    n = len(stream)
    print(f"python {platform.python_version()} on {platform.machine()}, {n:,} messages")
    print("| stage | messages/s | events/s | records/s |")
    print("|---|---:|---:|---:|")
    print(f"| decode | {n / decode_s:,.0f} | | |")
    print(
        f"| decode + assemble + route | {n / full_s:,.0f} | {events / full_s:,.0f} | "
        f"{records / full_s:,.0f} |"
    )


if __name__ == "__main__":
    main()
