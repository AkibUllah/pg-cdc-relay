"""Log sequence numbers: Postgres prints them as ``X/Y`` (two 32-bit hex halves)."""

from __future__ import annotations

import re

_LSN = re.compile(r"^([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})$")


def parse_lsn(text: str) -> int:
    match = _LSN.match(text.strip())
    if not match:
        raise ValueError(f"not an LSN: {text!r}")
    return (int(match.group(1), 16) << 32) | int(match.group(2), 16)


def format_lsn(value: int) -> str:
    if not 0 <= value < 1 << 64:
        raise ValueError(f"LSN out of range: {value}")
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"
