"""JSON Lines sinks: stdout for piping, a file for local consumers and dead letters."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Sequence
from typing import IO, Any

from cdc_relay.routing import Record
from cdc_relay.sinks.base import RetryableSinkError


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


class StdoutSink:
    name = "stdout"

    def __init__(self, stream: IO[str] | None = None) -> None:
        self.stream = stream or sys.stdout

    def send(self, records: Sequence[Record]) -> None:
        self.stream.write("".join(dumps(r.to_dict()) + "\n" for r in records))
        self.stream.flush()

    def close(self) -> None:
        pass


class FileSink:
    name = "file"

    def __init__(self, path: str, *, fsync: bool = True) -> None:
        self.path = path
        self.fsync = fsync
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8")  # noqa: SIM115 - kept open for the run

    def send(self, records: Sequence[Record]) -> None:
        self.write_lines(dumps(r.to_dict()) for r in records)

    def write_lines(self, lines) -> None:
        try:
            self._fh.write("".join(line + "\n" for line in lines))
            self._fh.flush()
            if self.fsync:
                os.fsync(self._fh.fileno())
        except OSError as exc:
            raise RetryableSinkError(f"writing {self.path}: {exc}") from exc

    def close(self) -> None:
        self._fh.close()
