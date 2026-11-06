"""HTTP server exposing /healthz, /readyz, and /metrics for eval worker."""

import http.server
import logging
import threading
from typing import Callable, Optional

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

logger = logging.getLogger(__name__)


class HealthHandler(http.server.BaseHTTPRequestHandler):
    """Handler for liveness, readiness, and metrics endpoints."""

    ready_check: Callable[[], bool] = lambda: True

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok\n")
        elif self.path == "/readyz":
            if self.ready_check():
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"ready\n")
            else:
                self.send_response(503)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"dependencies not ready\n")
        elif self.path == "/metrics":
            data = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"not found\n")

    def log_message(self, format: str, *args: object) -> None:
        # Suppress verbose HTTP access logs in production
        logger.debug("%s - - [%s] %s", self.address_string(), self.log_date_time_string(), format % args)


class HealthServer:
    """Runs HealthHandler on a background thread."""

    def __init__(
        self,
        addr: str = ":8085",
        ready_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        host, port_str = addr.rsplit(":", 1) if ":" in addr else ("", addr)
        port = int(port_str) if port_str else 8085
        self.host = host
        self.port = port
        self.ready_check = ready_check if ready_check is not None else (lambda: True)
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the HTTP server on a daemon thread."""
        handler_cls = type(
            "ConfiguredHealthHandler",
            (HealthHandler,),
            {"ready_check": staticmethod(self.ready_check)},
        )
        self._server = http.server.ThreadingHTTPServer((self.host, self.port), handler_cls)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info("Health server listening on %s:%d", self.host or "0.0.0.0", self.port)

    def stop(self) -> None:
        """Shut down the HTTP server."""
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            logger.info("Health server stopped")

