import contextlib
import http.client
import io
import socket
import struct
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest import mock

import freellmapi_retry as fr

EXHAUSTED = (
    b"API Error: Request rejected (429) - All models exhausted: 1 route checked "
    b"(1 rate-limited or on cooldown). Soonest reset ~1s."
)


class Recorder:
    """Shared state for a stub upstream: captured requests and scripted replies."""

    def __init__(self, replies):
        self.replies = list(replies)
        self.requests = []
        self.lock = threading.Lock()

    def next_reply(self, handler, body):
        with self.lock:
            self.requests.append(
                {
                    "method": handler.command,
                    "path": handler.path,
                    "headers": dict(handler.headers.items()),
                    "body": body,
                }
            )
            return self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]


def make_stub(recorder, stream=False):
    class Stub(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _record_body(self):
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        def do_POST(self):
            body = self._record_body()
            status, payload = recorder.next_reply(self, body)
            if stream and status == 200:
                self._send_stream()
                return
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _send_stream(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for i in range(3):
                event = b'data: {"i": %d}\n\n' % i
                self.wfile.write(b"%x\r\n%s\r\n" % (len(event), event))
                self.wfile.flush()
                time.sleep(0.2)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()

    return Stub


def start(server):
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


class ProxyTestCase(unittest.TestCase):
    def setUp(self):
        self._saved_extra = fr.RETRY_EXTRA_WAIT
        fr.RETRY_EXTRA_WAIT = 0
        self.servers = []

    def tearDown(self):
        for server in self.servers:
            server.shutdown()
            server.server_close()
        fr.RETRY_EXTRA_WAIT = self._saved_extra

    def start_proxy(self, upstream, **kwargs):
        kwargs.setdefault("default_cooldown", 1)
        kwargs.setdefault("max_retries", 3)
        cfg = fr.Config(
            upstream=fr.Upstream("http", "127.0.0.1", upstream.server_address[1]),
            port=0,
            **kwargs,
        )
        server = start(fr.make_server(cfg))
        self.servers.append(server)
        return server

    def start_stub(self, recorder, stream=False):
        server = start(
            ThreadingHTTPServer(("127.0.0.1", 0), make_stub(recorder, stream))
        )
        self.servers.append(server)
        return server

    def do_request(self, proxy, body=b'{"x":1}', path="/v1/messages", headers=None):
        conn = http.client.HTTPConnection(
            "127.0.0.1", proxy.server_address[1], timeout=30
        )
        try:
            hdrs = {"Content-Type": "application/json"}
            hdrs.update(headers or {})
            conn.request("POST", path, body=body, headers=hdrs)
            resp = conn.getresponse()
            return resp.status, dict(resp.getheaders()), resp.read()
        finally:
            conn.close()


class ParsingTests(unittest.TestCase):
    def test_wants_stream_from_body_and_accept(self):
        handler: Any = object.__new__(fr.ProxyHandler)
        for accept, body, expected in (
            (None, b'{"stream":true}', True),
            (None, b'{"stream": false}', False),
            (None, b'{"stream":true,"model":"deepseek"}', True),
            (None, b'not json at all {"stream":true}', False),
            (None, b'{"model":"deepseek"}', False),
            ("text/event-stream", b'{"x":1}', True),
        ):
            handler.headers = {"Accept": accept} if accept else {}
            self.assertEqual(
                handler._wants_stream(body),  # pylint: disable=protected-access
                expected,
                (accept, body),
            )

    def test_cooldown_parsed_from_body(self):
        self.assertEqual(fr.parse_cooldown(EXHAUSTED, 90), 1)

    def test_cooldown_parses_units(self):
        for token, expected in [
            (b"~90s", 90),
            (b"~4m", 240),
            (b"~1m30s", 90),
            (b"~2m 15s", 135),
            (b"~1.5s", 2),
            (b"~0s", 0),
        ]:
            body = b"All models exhausted. Soonest reset " + token + b"."
            self.assertEqual(fr.parse_cooldown(body, 90), expected, token)

    def test_cooldown_falls_back_when_absent(self):
        self.assertEqual(fr.parse_cooldown(b"429 slow down", 90), 90)
        self.assertEqual(fr.parse_cooldown(b"soonest reset ~", 90), 90)

    def test_marker_detection(self):
        self.assertTrue(fr.is_retryable_429(EXHAUSTED))
        self.assertFalse(fr.is_retryable_429(b"rate limited, try later"))

    def test_upstream_parsing(self):
        up = fr.parse_upstream("http://127.0.0.1:31415")
        self.assertEqual((up.scheme, up.host, up.port), ("http", "127.0.0.1", 31415))
        with self.assertRaises(ValueError):
            fr.parse_upstream("ftp://example.com")


class NotificationTests(unittest.TestCase):
    def test_skipped_off_macos(self):
        with (
            mock.patch.object(fr.sys, "platform", "linux"),
            mock.patch.object(fr.subprocess, "Popen") as popen,
        ):
            fr.notify_rate_limited(91, 1, 10)
        popen.assert_not_called()

    def test_invokes_osascript_on_macos(self):
        with (
            mock.patch.object(fr.sys, "platform", "darwin"),
            mock.patch.object(fr.subprocess, "Popen") as popen,
        ):
            fr.notify_rate_limited(91, 1, 10)

        cmd = popen.call_args.args[0]
        self.assertEqual(cmd[0], "osascript")
        self.assertIn("display notification", cmd[2])
        self.assertEqual(cmd[3], fr.NOTIFY_TITLE)
        self.assertIn("retry 1/10 in 91s", cmd[4])

    def test_missing_osascript_is_swallowed(self):
        with (
            mock.patch.object(fr.sys, "platform", "darwin"),
            mock.patch.object(
                fr.subprocess, "Popen", side_effect=OSError("no osascript")
            ),
        ):
            fr.notify_rate_limited(91, 1, 10)


class ServerErrorTests(unittest.TestCase):
    """Client disconnects are noise; anything else must still be reported."""

    def setUp(self):
        self.server = fr.ProxyServer(("127.0.0.1", 0), fr.ProxyHandler)
        self.server.cfg = fr.Config(
            upstream=fr.Upstream("http", "127.0.0.1", 9), port=0
        )

    def tearDown(self):
        self.server.server_close()

    def _handle_error(self, exc):
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            try:
                raise exc
            except type(exc):
                self.server.handle_error(None, ("127.0.0.1", 12345))
        return buf.getvalue()

    def test_client_disconnects_are_not_reported(self):
        for exc in (
            ConnectionResetError(54, "Connection reset by peer"),
            BrokenPipeError(32, "Broken pipe"),
        ):
            self.assertEqual(self._handle_error(exc), "")

    def test_unexpected_errors_still_surface(self):
        self.assertIn("boom", self._handle_error(RuntimeError("boom")))


class ProxyBehaviourTests(ProxyTestCase):
    def test_passthrough_preserves_request(self):
        recorder = Recorder([(200, b'{"ok":true}')])
        stub = self.start_stub(recorder)
        proxy = self.start_proxy(stub)

        status, headers, body = self.do_request(
            proxy,
            body=b'{"model":"deepseek"}',
            path="/v1/messages?beta=1",
            headers={
                "Authorization": "Bearer sk-test",
                "anthropic-version": "2023-06-01",
            },
        )

        self.assertEqual(status, 200)
        self.assertEqual(body, b'{"ok":true}')
        self.assertEqual(headers["Content-Type"], "application/json")
        seen = recorder.requests[0]
        self.assertEqual(seen["method"], "POST")
        self.assertEqual(seen["path"], "/v1/messages?beta=1")
        self.assertEqual(seen["body"], b'{"model":"deepseek"}')
        self.assertEqual(seen["headers"]["Authorization"], "Bearer sk-test")
        self.assertEqual(seen["headers"]["anthropic-version"], "2023-06-01")
        self.assertEqual(seen["headers"]["Content-Length"], "20")

    def test_retries_exhausted_429_then_succeeds(self):
        recorder = Recorder([(429, EXHAUSTED), (200, b'{"ok":true}')])
        stub = self.start_stub(recorder)
        proxy = self.start_proxy(stub)

        started = time.monotonic()
        status, _, body = self.do_request(proxy)
        elapsed = time.monotonic() - started

        self.assertEqual(status, 200)
        self.assertEqual(body, b'{"ok":true}')
        self.assertEqual(len(recorder.requests), 2)
        self.assertGreaterEqual(elapsed, 1.0)

    def test_streaming_retry_sends_keepalive_comments(self):
        recorder = Recorder([(429, EXHAUSTED), (200, b"")])
        stub = self.start_stub(recorder, stream=True)
        proxy = self.start_proxy(stub, keepalive_interval=0.2)

        status, headers, body = self.do_request(proxy, body=b'{"stream":true}')

        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/event-stream; charset=utf-8")
        self.assertIn(b": upstream rate-limited (all models exhausted)", body)
        self.assertIn(b": still waiting", body)
        self.assertEqual(body.count(b"data:"), 3)

    def test_keepalive_can_be_disabled(self):
        recorder = Recorder([(429, EXHAUSTED), (200, b"")])
        stub = self.start_stub(recorder, stream=True)
        proxy = self.start_proxy(stub, keepalive_interval=0)

        status, headers, body = self.do_request(proxy, body=b'{"stream":true}')

        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/event-stream")
        self.assertNotIn(b": upstream rate-limited", body)
        self.assertEqual(body.count(b"data:"), 3)

    def test_keepalive_uses_accept_header(self):
        recorder = Recorder([(429, EXHAUSTED), (200, b"")])
        stub = self.start_stub(recorder, stream=True)
        proxy = self.start_proxy(stub)

        status, _, body = self.do_request(
            proxy, body=b'{"x":1}', headers={"Accept": "text/event-stream"}
        )

        self.assertEqual(status, 200)
        self.assertIn(b": upstream rate-limited", body)

    def test_exhausted_retries_after_keepalive_yield_stream_error(self):
        recorder = Recorder([(429, EXHAUSTED)])
        stub = self.start_stub(recorder, stream=True)
        proxy = self.start_proxy(stub, max_retries=1)

        status, headers, body = self.do_request(proxy, body=b'{"stream":true}')

        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/event-stream; charset=utf-8")
        self.assertIn(b"event: error", body)
        self.assertIn(b"All models exhausted", body)

    def test_notifies_once_on_first_retryable_429(self):
        recorder = Recorder([(429, EXHAUSTED)])
        stub = self.start_stub(recorder)
        proxy = self.start_proxy(stub, max_retries=2, notify=True)

        with mock.patch.object(fr, "notify_rate_limited") as notify:
            status, _, _ = self.do_request(proxy)

        self.assertEqual(status, 429)
        notify.assert_called_once_with(1, 1, 2)

    def test_notifications_are_off_by_default(self):
        recorder = Recorder([(429, EXHAUSTED), (200, b'{"ok":true}')])
        stub = self.start_stub(recorder)
        proxy = self.start_proxy(stub)

        with mock.patch.object(fr, "notify_rate_limited") as notify:
            status, _, _ = self.do_request(proxy)

        self.assertEqual(status, 200)
        notify.assert_not_called()

    def test_non_streaming_client_gets_no_keepalive(self):
        recorder = Recorder([(429, EXHAUSTED), (200, b'{"ok":true}')])
        stub = self.start_stub(recorder)
        proxy = self.start_proxy(stub)

        status, headers, body = self.do_request(proxy, body=b'{"x":1}')

        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(body, b'{"ok":true}')

    def test_other_429_is_relayed_untouched(self):
        other = b'{"error":"slow down"}'
        recorder = Recorder([(429, other), (200, b'{"ok":true}')])
        stub = self.start_stub(recorder)
        proxy = self.start_proxy(stub)

        status, _, body = self.do_request(proxy)

        self.assertEqual(status, 429)
        self.assertEqual(body, other)
        self.assertEqual(len(recorder.requests), 1)

    def test_retry_cap_returns_last_429(self):
        recorder = Recorder([(429, EXHAUSTED)])
        stub = self.start_stub(recorder)
        proxy = self.start_proxy(stub, max_retries=1)

        status, _, body = self.do_request(proxy)

        self.assertEqual(status, 429)
        self.assertIn(b"All models exhausted", body)
        self.assertEqual(len(recorder.requests), 2)

    def test_502_when_upstream_unreachable(self):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        dead_port = probe.getsockname()[1]
        probe.close()

        cfg = fr.Config(upstream=fr.Upstream("http", "127.0.0.1", dead_port), port=0)
        proxy = start(fr.make_server(cfg))
        self.servers.append(proxy)

        status, _, body = self.do_request(proxy)
        self.assertEqual(status, 502)
        self.assertIn(b"upstream unreachable", body)

    def test_client_reset_produces_no_traceback(self):
        stub = self.start_stub(Recorder([(200, b'{"ok":true}')]))
        proxy = self.start_proxy(stub)

        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            # Connect, then RST without sending a request: the proxy is blocked
            # in readline and gets ConnectionResetError from the socket.
            sock = socket.create_connection(
                ("127.0.0.1", proxy.server_address[1]), timeout=10
            )
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
            sock.close()
            time.sleep(0.3)

        self.assertNotIn("Traceback", buf.getvalue())

    def test_chunked_request_body_is_forwarded(self):
        recorder = Recorder([(200, b'{"ok":true}')])
        stub = self.start_stub(recorder)
        proxy = self.start_proxy(stub)

        sock = socket.create_connection(
            ("127.0.0.1", proxy.server_address[1]), timeout=10
        )
        try:
            sock.sendall(
                b"POST /v1/messages HTTP/1.1\r\n"
                b"Host: proxy\r\n"
                b"Transfer-Encoding: chunked\r\n"
                b"Content-Type: application/json\r\n\r\n"
                b"5\r\nhello\r\n0\r\n\r\n"
            )
            raw = b""
            while b"\r\n\r\n" not in raw:
                raw += sock.recv(4096)
            head, _, body = raw.partition(b"\r\n\r\n")
            length = 0
            for line in head.split(b"\r\n"):
                if line.lower().startswith(b"content-length:"):
                    length = int(line.split(b":", 1)[1])
            while len(body) < length:
                body += sock.recv(4096)
        finally:
            sock.close()

        self.assertIn(b"200 OK", head)
        self.assertEqual(recorder.requests[0]["body"], b"hello")

    def test_streaming_body_arrives_incrementally(self):
        recorder = Recorder([(200, b"")])
        stub = self.start_stub(recorder, stream=True)
        proxy = self.start_proxy(stub)

        conn = http.client.HTTPConnection(
            "127.0.0.1", proxy.server_address[1], timeout=30
        )
        try:
            conn.request(
                "POST",
                "/v1/messages",
                body=b"{}",
                headers={"Content-Type": "application/json", "Content-Length": "2"},
            )
            resp = conn.getresponse()
            self.assertEqual(resp.getheader("Content-Type"), "text/event-stream")
            arrivals, data = [], b""
            while True:
                chunk = resp.read1(65536)
                if not chunk:
                    break
                arrivals.append(time.monotonic())
                data += chunk
        finally:
            conn.close()

        self.assertEqual(data.count(b"data:"), 3)
        self.assertGreaterEqual(len(arrivals), 3)
        self.assertGreaterEqual(arrivals[-1] - arrivals[0], 0.3)


if __name__ == "__main__":
    unittest.main()
