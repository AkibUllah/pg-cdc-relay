import pytest

from cdc_relay.lsn import format_lsn, parse_lsn


@pytest.mark.parametrize(
    ("text", "value"),
    [("0/0", 0), ("0/16B3748", 0x16B3748), ("16/B374D848", (0x16 << 32) | 0xB374D848)],
)
def test_round_trip(text: str, value: int) -> None:
    assert parse_lsn(text) == value
    assert format_lsn(value) == text


def test_parse_is_case_insensitive_and_strips() -> None:
    assert parse_lsn(" 1/abcdef ") == (1 << 32) | 0xABCDEF


@pytest.mark.parametrize("bad", ["", "16", "0/", "/1", "g/1", "1/2/3", "123456789/0"])
def test_parse_rejects_garbage(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_lsn(bad)


def test_format_rejects_out_of_range() -> None:
    with pytest.raises(ValueError):
        format_lsn(-1)
    with pytest.raises(ValueError):
        format_lsn(1 << 64)
