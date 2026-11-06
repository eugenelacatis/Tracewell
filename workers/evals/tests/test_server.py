"""Unit tests for eval worker health and metrics handler logic."""

import io

from tracewell_evals.server import HealthHandler


class FakeHTTPRequest:
    def makefile(self, *args: object, **kwargs: object) -> io.BytesIO:
        return io.BytesIO()

    def sendall(self, *args: object) -> None:
        pass


def call_handler(path: str, ready_fn: object = None) -> tuple[int, bytes, dict[str, str]]:
    """Simulate an HTTP GET request to HealthHandler without TCP sockets."""
    handler_cls = type(
        "MockHealthHandler",
        (HealthHandler,),
        {"ready_check": staticmethod(ready_fn if ready_fn is not None else (lambda: True))},
    )

    request_bytes = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("utf-8")
    rfile = io.BytesIO(request_bytes)
    wfile = io.BytesIO()

    handler = handler_cls.__new__(handler_cls)
    handler.rfile = rfile
    handler.wfile = wfile
    handler.requestline = f"GET {path} HTTP/1.1"
    handler.command = "GET"
    handler.path = path
    handler.request_version = "HTTP/1.1"
    handler.headers = {}
    handler.close_connection = True
    handler.connection = FakeHTTPRequest()

    status_code = [200]
    headers_dict: dict[str, str] = {}

    def mock_send_response(code: int, message: str = "") -> None:
        status_code[0] = code

    def mock_send_header(keyword: str, value: str) -> None:
        headers_dict[keyword] = value

    def mock_end_headers() -> None:
        pass

    handler.send_response = mock_send_response
    handler.send_header = mock_send_header
    handler.end_headers = mock_end_headers

    handler.do_GET()

    return status_code[0], wfile.getvalue(), headers_dict


def test_healthz_endpoint() -> None:
    code, body, headers = call_handler("/healthz")
    assert code == 200
    assert body == b"ok\n"
    assert "Content-Type" in headers


def test_readyz_endpoint_ready() -> None:
    code, body, _ = call_handler("/readyz", ready_fn=lambda: True)
    assert code == 200
    assert body == b"ready\n"


def test_readyz_endpoint_not_ready() -> None:
    code, body, _ = call_handler("/readyz", ready_fn=lambda: False)
    assert code == 503
    assert body == b"dependencies not ready\n"


def test_metrics_endpoint() -> None:
    code, body, headers = call_handler("/metrics")
    assert code == 200
    content = body.decode("utf-8")
    assert "evals_messages_processed_total" in content
    assert "evals_produced_total" in content
    assert "evals_dead_letters_total" in content


def test_not_found_endpoint() -> None:
    code, body, _ = call_handler("/unknown")
    assert code == 404
