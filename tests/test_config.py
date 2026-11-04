from pathlib import Path

import pytest

from cdc_relay.config import ConfigError, build_sink, load_settings, normalise_dsn
from cdc_relay.sinks import FileSink, RedisStreamSink, StdoutSink, WebhookSink

EXAMPLE = str(Path(__file__).resolve().parent.parent / "examples" / "relay.yaml")


def write(tmp_path, text: str) -> str:
    path = tmp_path / "relay.yaml"
    path.write_text(text)
    return str(path)


def test_defaults_without_a_file() -> None:
    s = load_settings(env={})
    assert (s.slot, s.publication, s.sink.type, s.status_port) == (
        "cdc_relay",
        "cdc_relay",
        "stdout",
        8086,
    )
    assert s.routing.outbox.table == "public.outbox"


def test_example_config_loads() -> None:
    s = load_settings(EXAMPLE, env={})
    assert s.routing.topic_template == "swift.cdc.{table}"
    assert s.routing.outbox.topic_prefix == "swift.events."
    assert s.publication_tables[0] == "public.parcels"


def test_environment_overrides_file_and_nested_keys(tmp_path) -> None:
    path = write(tmp_path, "slot: from_file\nsink:\n  type: stdout\n")
    s = load_settings(
        path,
        env={
            "CDC_SLOT": "from_env",
            "CDC_SINK": "webhook",
            "CDC_WEBHOOK_URL": "https://hooks.example/cdc",
            "CDC_WEBHOOK_SECRET": "whsec",
            "CDC_STATUS_PORT": "0",
            "CDC_MASK_SALT": "salt",
        },
    )
    assert (s.slot, s.sink.type, s.sink.url, s.sink.secret) == (
        "from_env",
        "webhook",
        "https://hooks.example/cdc",
        "whsec",
    )
    assert s.status_port == 0 and s.routing.mask_salt == "salt"


def test_config_path_from_env_and_database_url_fallback(tmp_path) -> None:
    path = write(tmp_path, "publication: pub_a\n")
    s = load_settings(
        env={"CDC_CONFIG": path, "DATABASE_URL": "postgresql+psycopg2://u:p@db:5432/app"}
    )
    assert s.publication == "pub_a"
    assert s.dsn == "postgresql://u:p@db:5432/app"


@pytest.mark.parametrize(
    ("text", "error"),
    [
        ("slott: x\n", "unknown config keys: slott"),
        ("routing:\n  mask_colums: {}\n", "unknown routing keys: mask_colums"),
        ("routing:\n  outbox:\n    tabel: x\n", "unknown routing.outbox keys: tabel"),
        ("sink:\n  type: kafka\n", "sink.type must be one of"),
        ("sink:\n  type: webhook\n", "needs sink.url"),
        ("sink:\n  type: redis\n", "needs sink.redis_url"),
        ("slot: Bad-Name\n", "slot must be a lowercase identifier"),
        ("batch_max_records: 0\n", "batch_max_records"),
        ("routing:\n  mask_columns: {public.t: [a]}\n", "mask_salt"),
        ("- a list\n", "must be a mapping"),
        ("slot: [unclosed\n", "invalid YAML"),
    ],
)
def test_invalid_configs(tmp_path, text: str, error: str) -> None:
    with pytest.raises(ConfigError, match=error):
        load_settings(write(tmp_path, text), env={})


def test_bad_env_values_and_missing_file(tmp_path) -> None:
    with pytest.raises(ConfigError, match="CDC_STATUS_PORT"):
        load_settings(env={"CDC_STATUS_PORT": "eighty"})
    with pytest.raises(ConfigError, match="cannot read config"):
        load_settings(str(tmp_path / "missing.yaml"), env={})


def test_normalise_dsn() -> None:
    assert normalise_dsn("postgres+asyncpg://a@b/c") == "postgresql://a@b/c"
    assert normalise_dsn("postgres://a@b/c") == "postgres://a@b/c"


def test_build_sink_for_each_type(tmp_path) -> None:
    s = load_settings(env={})
    assert isinstance(build_sink(s.sink), StdoutSink)
    s.sink.type, s.sink.path = "file", str(tmp_path / "out.jsonl")
    sink = build_sink(s.sink)
    assert isinstance(sink, FileSink)
    sink.close()
    s.sink.type, s.sink.url = "webhook", "https://hooks.example/cdc"
    assert isinstance(build_sink(s.sink), WebhookSink)
    s.sink.type, s.sink.redis_url = "redis", "redis://localhost:6390/0"
    assert isinstance(build_sink(s.sink), RedisStreamSink)  # connects lazily
