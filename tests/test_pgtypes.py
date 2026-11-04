import pytest

from cdc_relay import pgtypes as t
from cdc_relay.pgtypes import convert, parse_array


@pytest.mark.parametrize(
    ("oid", "text", "expected"),
    [
        (t.BOOL, "t", True),
        (t.BOOL, "f", False),
        (t.INT8, "9007199254740993", 9007199254740993),
        (t.INT2, "-5", -5),
        (t.FLOAT8, "1.5", 1.5),
        (t.FLOAT8, "NaN", "NaN"),
        (t.FLOAT4, "-Infinity", "-Infinity"),
        (t.NUMERIC, "1250.50", "1250.50"),
        (t.TEXT, "ঢাকা", "ঢাকা"),
        (t.UUID, "0b6c3d1e-7f0e-4d4a-9a64-0c1b2f3e4d5a", "0b6c3d1e-7f0e-4d4a-9a64-0c1b2f3e4d5a"),
        (t.JSONB, '{"amount": "120.00", "tags": ["cod"]}', {"amount": "120.00", "tags": ["cod"]}),
        (t.BYTEA, "\\xDEADbeef", "deadbeef"),
        (t.DATE, "2026-11-04", "2026-11-04"),
        (t.TIMESTAMP, "2026-11-04 10:15:30.5", "2026-11-04T10:15:30.5"),
        (t.TIMESTAMP, "infinity", "infinity"),
    ],
)
def test_scalars(oid: int, text: str, expected) -> None:
    assert convert(oid, text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2026-11-04 10:15:30+06", "2026-11-04T04:15:30Z"),
        ("2026-11-04 04:15:30.123+00", "2026-11-04T04:15:30.123000Z"),
        ("2026-11-04 10:45:30.000001+05:30", "2026-11-04T05:15:30.000001Z"),
        ("1900-01-01 00:00:00+05:53:28", "1899-12-31T18:06:32Z"),
        ("-infinity", "-infinity"),
    ],
)
def test_timestamptz_is_normalised_to_utc(text: str, expected: str) -> None:
    assert convert(t.TIMESTAMPTZ, text) == expected


def test_null_and_unknown_types() -> None:
    assert convert(t.INT4, None) is None
    assert convert(99999, "(1,2]") == "(1,2]"


def test_values_that_do_not_parse_pass_through_as_text() -> None:
    assert convert(t.INT4, "12abc") == "12abc"
    assert convert(t.JSONB, "{broken") == "{broken"
    assert convert(t.BYTEA, "plain") == "plain"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("{}", []),
        ("{1,2,3}", ["1", "2", "3"]),
        ('{a,"b c",NULL,"NULL","q\\"x","back\\\\slash"}', ["a", "b c", None, "NULL", 'q"x', "back\\slash"]),
        ("[0:1]={7,8}", ["7", "8"]),
    ],
)
def test_parse_array(text: str, expected) -> None:
    assert parse_array(text) == expected


def test_typed_arrays() -> None:
    assert convert(1007, "{1,NULL,3}") == [1, None, 3]
    assert convert(1009, '{bkash,"cash on delivery"}') == ["bkash", "cash on delivery"]
    assert convert(1000, "{t,f}") == [True, False]
    assert convert(1007, "{{1,2},{3,4}}") == "{{1,2},{3,4}}"
