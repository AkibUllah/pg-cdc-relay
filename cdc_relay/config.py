"""Settings from an optional YAML file, overridden by environment variables.

Unknown keys are an error rather than silently ignored: a typo in ``mask_columns`` would
otherwise publish the column you meant to hide.
"""

from __future__ import annotations

import dataclasses
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import yaml

from cdc_relay.routing import OutboxConfig, RoutingConfig

SINK_TYPES = ("stdout", "file", "webhook", "redis")
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class ConfigError(ValueError):
    pass


@dataclass
class SinkConfig:
    type: str = "stdout"
    url: str | None = None  # webhook
    secret: str | None = None  # webhook HMAC secret
    timeout: float = 10.0
    max_batch: int = 500
    path: str | None = None  # file
    redis_url: str | None = None
    stream_template: str = "{topic}"
    maxlen: int | None = None


@dataclass
class Settings:
    dsn: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    slot: str = "cdc_relay"
    publication: str = "cdc_relay"
    # None publishes every table (FOR ALL TABLES, needs superuser); otherwise "schema.table" list.
    publication_tables: list[str] | None = None
    create_slot: bool = True
    checkpoint_path: str | None = "relay.checkpoint.json"
    dead_letter_path: str | None = None
    batch_max_records: int = 500
    batch_max_wait: float = 0.5
    status_interval: float = 10.0
    status_host: str = "0.0.0.0"
    status_port: int = 8086  # 0 disables the status server
    lag_check_interval: float = 30.0
    log_level: str = "INFO"
    sink: SinkConfig = field(default_factory=SinkConfig)
    routing: RoutingConfig = field(default_factory=RoutingConfig)

    def validate(self) -> Settings:
        for name in ("slot", "publication"):
            if not _IDENTIFIER.match(getattr(self, name)):
                raise ConfigError(
                    f"{name} must be a lowercase identifier (letters, digits, _), "
                    f"got {getattr(self, name)!r}"
                )
        if self.sink.type not in SINK_TYPES:
            raise ConfigError(f"sink.type must be one of {', '.join(SINK_TYPES)}")
        required = {"webhook": "url", "file": "path", "redis": "redis_url"}.get(self.sink.type)
        if required and not getattr(self.sink, required):
            raise ConfigError(f"sink.type {self.sink.type} needs sink.{required}")
        if self.batch_max_records < 1 or self.batch_max_wait < 0:
            raise ConfigError("batch_max_records must be >= 1 and batch_max_wait >= 0")
        if not 0 <= self.status_port <= 65535:
            raise ConfigError("status_port must be 0..65535")
        return self


def _build(cls: type, data: Mapping[str, Any], where: str) -> Any:
    if not isinstance(data, Mapping):
        raise ConfigError(f"{where} must be a mapping")
    known = {f.name: f for f in dataclasses.fields(cls)}
    unknown = sorted(set(data) - set(known))
    if unknown:
        raise ConfigError(f"unknown {where} keys: {', '.join(unknown)}")
    kwargs = dict(data)
    nested = {
        Settings: {"sink": SinkConfig, "routing": RoutingConfig},
        RoutingConfig: {"outbox": OutboxConfig},
    }.get(cls, {})
    for key, sub in nested.items():
        if key in kwargs:
            label = key if where == "config" else f"{where}.{key}"
            kwargs[key] = _build(sub, kwargs[key] or {}, label)
    try:
        return cls(**kwargs)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{where}: {exc}") from exc


ENV = {
    "CDC_DSN": ("dsn", str),
    "CDC_SLOT": ("slot", str),
    "CDC_PUBLICATION": ("publication", str),
    "CDC_CHECKPOINT_PATH": ("checkpoint_path", str),
    "CDC_DEAD_LETTER_PATH": ("dead_letter_path", str),
    "CDC_STATUS_PORT": ("status_port", int),
    "CDC_LOG_LEVEL": ("log_level", str),
    "CDC_SINK": ("sink.type", str),
    "CDC_WEBHOOK_URL": ("sink.url", str),
    "CDC_WEBHOOK_SECRET": ("sink.secret", str),
    "CDC_SINK_PATH": ("sink.path", str),
    "CDC_REDIS_URL": ("sink.redis_url", str),
    "CDC_MASK_SALT": ("routing.mask_salt", str),
}


def normalise_dsn(dsn: str) -> str:
    """Accept SQLAlchemy-style URLs (postgresql+psycopg2://) that other services share."""
    return re.sub(r"^postgres(?:ql)?\+[a-z0-9]+://", "postgresql://", dsn)


def load_settings(path: str | None = None, env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env
    data: dict[str, Any] = {}
    path = path or env.get("CDC_CONFIG")
    if path:
        try:
            with open(path, encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        except OSError as exc:
            raise ConfigError(f"cannot read config {path}: {exc}") from exc
        except yaml.YAMLError as exc:
            raise ConfigError(f"invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("config file must be a mapping")
    if "dsn" not in data and env.get("DATABASE_URL"):
        data["dsn"] = env["DATABASE_URL"]
    for var, (dotted, kind) in ENV.items():
        if env.get(var) in (None, ""):
            continue
        try:
            value = kind(env[var])
        except ValueError as exc:
            raise ConfigError(f"{var}: {exc}") from exc
        target = data
        *parents, leaf = dotted.split(".")
        for parent in parents:
            child = target.get(parent)
            if not isinstance(child, dict):
                child = target[parent] = {}
            target = child
        target[leaf] = value
    settings = _build(Settings, data, "config")
    settings.dsn = normalise_dsn(settings.dsn)
    return settings.validate()


def build_sink(config: SinkConfig):
    from cdc_relay.sinks import FileSink, RedisStreamSink, StdoutSink, WebhookSink

    if config.type == "stdout":
        return StdoutSink()
    if config.type == "file":
        return FileSink(config.path or "")
    if config.type == "webhook":
        return WebhookSink(
            config.url or "",
            secret=config.secret,
            timeout=config.timeout,
            max_batch=config.max_batch,
        )
    return RedisStreamSink.from_url(
        config.redis_url or "", stream_template=config.stream_template, maxlen=config.maxlen
    )
