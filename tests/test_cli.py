import json
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from cdc_relay.cli import main, read_capture
from cdc_relay.metrics import Metrics
from cdc_relay.server import StatusServer

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = str(ROOT / "tests" / "fixtures" / "demo_stream.jsonl")


@pytest.fixture(autouse=True)
def no_env_config(monkeypatch) -> None:
    for var in ("CDC_CONFIG", "CDC_DSN", "DATABASE_URL", "CDC_SINK"):
        monkeypatch.delenv(var, raising=False)


def test_decode_prints_one_json_line_per_message(capsys) -> None:
    assert main(["decode", FIXTURE]) == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(lines) == 51
    assert lines[0]["type"] == "Begin" and "/" in lines[0]["final_lsn"]
    message = next(line for line in lines if line["type"] == "LogicalMessage")
    assert json.loads(message["content"])["event_type"] == "RiderAssigned"
    assert any("<unchanged toast>" in line.get("new", []) for line in lines)


def test_decode_reports_bad_messages_and_keeps_going(tmp_path, capsys) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text('{"lsn": "0/10", "data": "5a"}\n\n{"lsn": "0/20", "data": "4200"}\n')
    main(["decode", str(path)])
    out = [json.loads(x) for x in capsys.readouterr().out.splitlines()]
    assert out[0] == {"at": "0/10", "error": "unknown message type 'Z'"}
    assert "truncated" in out[1]["error"]


def test_read_capture_rejects_other_files(tmp_path) -> None:
    path = tmp_path / "x.jsonl"
    path.write_text('{"lsn": "nope", "data": ""}\n')
    with pytest.raises(SystemExit, match="x.jsonl:1"):
        list(read_capture(str(path)))


def test_replay_to_a_file_with_the_example_config(tmp_path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)  # the example config keeps its dead letter file in ./state
    out = tmp_path / "records.jsonl"
    code = main(
        [
            "-c",
            str(ROOT / "examples" / "relay.yaml"),
            "replay",
            FIXTURE,
            "--sink",
            "file",
            "--out",
            str(out),
        ]
    )
    assert code == 0
    assert "20 records delivered, 0 rejected" in capsys.readouterr().err
    topics = [json.loads(x)["topic"] for x in out.read_text().splitlines()]
    assert topics.count("swift.events.parcel") == 6


def test_config_errors_exit_2(tmp_path, capsys) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("slott: x\n")
    assert main(["-c", str(bad), "decode", FIXTURE]) == 2
    assert "unknown config keys" in capsys.readouterr().err


def get(url: str):
    try:
        with urllib.request.urlopen(url) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def test_status_server_endpoints() -> None:
    metrics = Metrics()
    metrics.inc("cdc_events_total", op="insert")
    state = {"healthy": True}
    server = StatusServer(
        "127.0.0.1",
        0,
        metrics=metrics,
        status=lambda: {"connected": True},
        healthy=lambda: state["healthy"],
    ).start()
    base = f"http://127.0.0.1:{server.port}"
    try:
        assert get(base + "/healthz") == (200, '{"status": "ok"}')
        state["healthy"] = False
        assert get(base + "/healthz")[0] == 503
        code, body = get(base + "/metrics")
        assert code == 200 and 'cdc_events_total{op="insert"} 1' in body
        assert json.loads(get(base + "/status")[1]) == {"connected": True}
        assert get(base + "/nope")[0] == 404
    finally:
        server.close()
