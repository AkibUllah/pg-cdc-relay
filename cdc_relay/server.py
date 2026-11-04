"""A small status server: /healthz for orchestrators, /metrics for Prometheus, /status JSON."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from cdc_relay.metrics import Metrics


class StatusServer:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        metrics: Metrics,
        status: Callable[[], dict[str, Any]],
        healthy: Callable[[], bool],
    ) -> None:
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - http.server naming
                path = self.path.split("?", 1)[0]
                if path == "/healthz":
                    ok = owner.healthy()
                    self._send(
                        200 if ok else 503,
                        "application/json",
                        json.dumps({"status": "ok" if ok else "unavailable"}),
                    )
                elif path == "/metrics":
                    self._send(200, "text/plain; version=0.0.4", owner.metrics.render())
                elif path == "/status":
                    self._send(
                        200, "application/json", json.dumps(owner.status(), default=str, indent=2)
                    )
                else:
                    self._send(404, "application/json", json.dumps({"error": "not found"}))

            def _send(self, code: int, content_type: str, body: str) -> None:
                data = body.encode()
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.metrics = metrics
        self.status = status
        self.healthy = healthy
        self.httpd = ThreadingHTTPServer((host, port), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    def start(self) -> StatusServer:
        self.thread.start()
        return self

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def heartbeat_health(source: Any, max_age: float) -> Callable[[], bool]:
    def healthy() -> bool:
        return bool(source.connected) and time.monotonic() - source.heartbeat < max_age

    return healthy
