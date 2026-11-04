"""Thread-safe counters and gauges rendered in the Prometheus text format."""

from __future__ import annotations

import threading
from collections import defaultdict

HELP = {
    "cdc_messages_total": ("counter", "pgoutput messages decoded, by message type."),
    "cdc_transactions_total": ("counter", "Committed transactions seen."),
    "cdc_transactions_skipped_total": (
        "counter",
        "Transactions skipped because the local checkpoint shows they were already delivered.",
    ),
    "cdc_events_total": ("counter", "Change events in committed transactions, by op."),
    "cdc_records_delivered_total": ("counter", "Records the sink accepted."),
    "cdc_records_rejected_total": ("counter", "Events the router rejected (bad outbox events)."),
    "cdc_records_dead_lettered_total": ("counter", "Records written to the dead letter file."),
    "cdc_delivery_retries_total": ("counter", "Batch deliveries retried after a sink error."),
    "cdc_batches_total": ("counter", "Batches delivered."),
    "cdc_batch_seconds_sum": ("counter", "Total time spent delivering batches, retries included."),
    "cdc_confirmed_lsn": ("gauge", "Last position confirmed to the server (as a number)."),
    "cdc_last_commit_timestamp_seconds": (
        "gauge",
        "Commit time of the last delivered transaction.",
    ),
    "cdc_slot_lag_bytes": ("gauge", "WAL the slot retains beyond its confirmed position."),
}


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[str, dict[tuple[tuple[str, str], ...], float]] = defaultdict(dict)

    def inc(self, name: str, amount: float = 1, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        with self._lock:
            series = self._values[name]
            series[key] = series.get(key, 0) + amount

    def set(self, name: str, value: float, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        with self._lock:
            self._values[name][key] = value

    def get(self, name: str, **labels: str) -> float:
        with self._lock:
            return self._values.get(name, {}).get(tuple(sorted(labels.items())), 0)

    def total(self, name: str) -> float:
        with self._lock:
            return sum(self._values.get(name, {}).values())

    def render(self) -> str:
        lines = []
        with self._lock:
            snapshot = {name: dict(series) for name, series in self._values.items()}
        for name in sorted(set(snapshot) | set(HELP)):
            kind, text = HELP.get(name, ("untyped", ""))
            lines.append(f"# HELP {name} {text}")
            lines.append(f"# TYPE {name} {kind}")
            series = snapshot.get(name) or ({(): 0.0} if kind == "counter" else {})
            for labels, value in sorted(series.items()):
                label_text = ",".join(f'{k}="{_escape(v)}"' for k, v in labels)
                suffix = f"{{{label_text}}}" if label_text else ""
                lines.append(f"{name}{suffix} {_number(value)}")
        return "\n".join(lines) + "\n"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _number(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else repr(value)
