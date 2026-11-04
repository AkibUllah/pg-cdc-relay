"""Convert pgoutput text-format values into JSON-safe Python values, by type OID.

pgoutput sends every value through the type's output function, so the relay sees exactly what
``psql`` would print. Conversions here keep information intact:

* ``numeric`` stays a string, since a float would silently round money amounts;
* ``timestamptz`` is normalised to UTC ISO 8601 with a ``Z`` suffix, whatever the session
  TimeZone was; ``infinity`` and ``-infinity`` pass through as strings;
* non-finite floats (``NaN``, ``Infinity``) stay strings because JSON has no spelling for them;
* ``json``/``jsonb`` are parsed, ``bytea`` becomes lowercase hex without the ``\\x`` prefix;
* one-dimensional arrays of the types above are parsed into lists.

Unknown types (enums, domains, ranges, extensions) are passed through as their text form.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

BOOL, BYTEA, INT8, INT2, INT4, TEXT, OID, JSON = 16, 17, 20, 21, 23, 25, 26, 114
FLOAT4, FLOAT8, BPCHAR, VARCHAR, DATE, TIME = 700, 701, 1042, 1043, 1082, 1083
TIMESTAMP, TIMESTAMPTZ, NUMERIC, UUID, JSONB = 1114, 1184, 1700, 2950, 3802

_TZ_SUFFIX = re.compile(r"([+-])(\d{2})(?::?(\d{2}))?(?::?(\d{2}))?$")


def _bool(text: str) -> bool:
    if text not in ("t", "f"):
        raise ValueError(f"not a boolean: {text!r}")
    return text == "t"


def _float(text: str) -> float | str:
    value = float(text)
    return value if math.isfinite(value) else text


def _bytea(text: str) -> str:
    if not text.startswith("\\x"):
        raise ValueError("bytea must use the hex output format (bytea_output = 'hex')")
    return text[2:].lower()


def _iso_naive(text: str) -> str:
    return text.replace(" ", "T", 1)


def _timestamptz(text: str) -> str:
    if text in ("infinity", "-infinity"):
        return text
    if text.endswith(" BC"):
        return text
    match = _TZ_SUFFIX.search(text)
    if not match:
        raise ValueError(f"timestamptz without offset: {text!r}")
    sign, hh, mm, ss = match.groups()
    offset = f"{sign}{hh}:{mm or '00'}" + (f":{ss}" if ss else "")
    base = text[: match.start()].replace(" ", "T", 1)
    if "." in base:  # fromisoformat on 3.10 wants exactly 3 or 6 fractional digits
        whole, frac = base.split(".", 1)
        base = f"{whole}.{frac.ljust(6, '0')[:6]}"
    parsed = datetime.fromisoformat(base + offset).astimezone(timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _timestamp(text: str) -> str:
    return text if text in ("infinity", "-infinity") else _iso_naive(text)


SCALARS: dict[int, Callable[[str], Any]] = {
    BOOL: _bool,
    INT2: int,
    INT4: int,
    INT8: int,
    OID: int,
    FLOAT4: _float,
    FLOAT8: _float,
    NUMERIC: str,
    TEXT: str,
    VARCHAR: str,
    BPCHAR: str,
    UUID: str,
    DATE: str,
    TIME: str,
    TIMESTAMP: _timestamp,
    TIMESTAMPTZ: _timestamptz,
    JSON: json.loads,
    JSONB: json.loads,
    BYTEA: _bytea,
}

# Array type OID -> element type OID (pg_type.typelem).
ARRAYS: dict[int, int] = {
    1000: BOOL,
    1001: BYTEA,
    1005: INT2,
    1007: INT4,
    1016: INT8,
    1028: OID,
    1021: FLOAT4,
    1022: FLOAT8,
    1231: NUMERIC,
    1009: TEXT,
    1015: VARCHAR,
    1014: BPCHAR,
    2951: UUID,
    1182: DATE,
    1115: TIMESTAMP,
    1185: TIMESTAMPTZ,
    199: JSON,
    3807: JSONB,
}


def parse_array(text: str) -> list[str | None]:
    """Split a one-dimensional array literal like ``{a,"b c",NULL}`` into element strings."""
    if text.startswith("["):  # explicit bounds, e.g. [0:1]={1,2}
        text = text[text.index("=") + 1 :]
    if not (text.startswith("{") and text.endswith("}")):
        raise ValueError(f"not an array literal: {text!r}")
    body = text[1:-1]
    if not body:
        return []
    if body.startswith("{"):
        raise ValueError("multi-dimensional arrays are passed through as text")
    items: list[str | None] = []
    i, n = 0, len(body)
    while i <= n:
        if i < n and body[i] == '"':
            i += 1
            chars = []
            while body[i] != '"':
                if body[i] == "\\":
                    i += 1
                chars.append(body[i])
                i += 1
            items.append("".join(chars))
            i += 1  # closing quote
        else:
            end = body.find(",", i)
            end = n if end < 0 else end
            raw = body[i:end].strip()
            items.append(None if raw.upper() == "NULL" else raw)
            i = end
        i += 1  # comma
    return items


def convert(type_oid: int, text: str | None) -> Any:
    """Convert one text value. Values that do not parse are returned unchanged as text."""
    if text is None:
        return None
    try:
        scalar = SCALARS.get(type_oid)
        if scalar is not None:
            return scalar(text)
        element = ARRAYS.get(type_oid)
        if element is not None:
            parse = SCALARS[element]
            return [None if v is None else parse(v) for v in parse_array(text)]
    except (ValueError, IndexError):
        return text
    return text
