#!/usr/bin/env python3
"""Transparent retrying proxy for FreeLLMAPI.

Relays every request to FreeLLMAPI verbatim, streaming responses straight
through. The only special case is FreeLLMAPI's pre-stream rate-limit response:

    429 {"error": "...All models exhausted... Soonest reset ~89s."}

Since that 429 is produced before any model output begins, a fresh request is
safe to replay. The proxy waits out the advertised cooldown and retries, so the
client never sees the error and never needs a manual "continue".

While waiting, a streaming request is held open with periodic SSE comments so a
long cooldown does not look like a stalled connection.

Stdlib only, single file, no dependencies.
"""

import argparse
import http.client
import json
import logging
import math
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

__version__ = "0.1.0"

DEFAULT_PORT = 31416
DEFAULT_UPSTREAM = "http://127.0.0.1:31415"
DEFAULT_MAX_RETRIES = 10
DEFAULT_COOLDOWN = 90
DEFAULT_KEEPALIVE = 10.0
UPSTREAM_TIMEOUT = 600.0
RETRY_EXTRA_WAIT = 2

EXHAUSTED_MARKER = b"all models exhausted"
RESET_RE = re.compile(
    rb"soonest reset ~((?:\d+m\s*)?(?:\d+(?:\.\d+)?s)?)", re.IGNORECASE
)
DURATION_RE = re.compile(rb"(\d+)m|(\d+(?:\.\d+)?)s", re.IGNORECASE)

# RFC 7230 hop-by-hop headers, plus Expect: we consume the request body
# ourselves, and forwarding Expect: 100-continue would make http.client hand us
# the interim 100 response instead of the real one.
HOP_BY_HOP = frozenset(
    {
        "connection",
        "expect",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)

log = logging.getLogger("freellmapi-retry")


class ClientGone(Exception):
    """The client hung up mid-response; nothing left to do for it."""


@dataclass(frozen=True)
class Upstream:
    scheme: str
    host: str
    port: int

    @property
    def authority(self):
        default = 443 if self.scheme == "https" else 80
        return self.host if self.port == default else "%s:%d" % (self.host, self.port)


@dataclass(frozen=True)
class Config:
    upstream: Upstream
    host: str = "127.0.0.1"
    port: int = DEFAULT_PORT
    max_retries: int = DEFAULT_MAX_RETRIES
    default_cooldown: int = DEFAULT_COOLDOWN
    keepalive_interval: float = DEFAULT_KEEPALIVE
    notify: bool = False


def parse_upstream(url):
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise ValueError("upstream must be an http:// or https:// URL, got %r" % url)
    if not parts.hostname:
        raise ValueError("upstream URL has no host: %r" % url)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return Upstream(parts.scheme, parts.hostname, port)


def parse_cooldown(body, default):
    match = RESET_RE.search(body)
    if not match or not match.group(1):
        return default
    total = 0
    for minutes, seconds in DURATION_RE.findall(match.group(1)):
        total += int(minutes) * 60 if minutes else 0
        total += math.ceil(float(seconds)) if seconds else 0
    return total


def is_retryable_429(body):
    return EXHAUSTED_MARKER in body.lower()


NOTIFY_TITLE = "FreeLLMAPI rate limited"


def notify_rate_limited(wait, attempt, max_retries):
    """Pop a macOS notification about a rate-limit retry.

    Best-effort: fire and forget so a slow or missing osascript never delays
    the retry. Title and message travel as separate argv entries, so neither
    is ever interpolated into the AppleScript source.
    """
    if sys.platform != "darwin":
        return
    message = "All models exhausted; retry %d/%d in %ds" % (
        attempt,
        max_retries,
        wait,
    )
    script = (
        "on run argv\n"
        "display notification (item 2 of argv) with title (item 1 of argv)\n"
        "end run"
    )
    try:
        # Fire and forget: the child is never waited on, by design.
        subprocess.Popen(  # pylint: disable=consider-using-with
            ["osascript", "-e", script, NOTIFY_TITLE, message],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        log.debug("could not show rate-limit notification: %s", exc)


class ProxyHandler(BaseHTTPRequestHandler):
    server: "ProxyServer"
    protocol_version = "HTTP/1.1"
    server_version = "freellmapi-retry/" + __version__
    sys_version = ""

    def setup(self):
        super().setup()
        self._response_started = False
        self._stream_committed = False
        try:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass

    def log_message(self, fmt, *args):
        log.debug("%s %s", self.address_string(), fmt % args)

    def do_GET(self):
        self._proxy()

    do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = do_HEAD = do_GET

    def do_CONNECT(self):
        self._send_text(501, "CONNECT is not supported\n")

    # -- request ----------------------------------------------------------

    def _proxy(self):
        cfg = self.server.cfg
        try:
            body = self._read_request_body()
        except (ValueError, OSError) as exc:
            self._send_text(400, "could not read request body: %s\n" % exc)
            return

        target = self._target_path()
        headers = self._forward_headers(cfg.upstream, body)
        attempt = 0
        # Only a client that asked for a stream can be kept warm while we wait:
        # anything else would see SSE framing where it expects a JSON body.
        can_hold_stream = (
            cfg.keepalive_interval > 0
            and self.command != "HEAD"
            and self._wants_stream(body)
        )

        try:
            while True:
                conn = self._connect(cfg.upstream)
                try:
                    conn.request(self.command, target, body=body, headers=headers)
                    resp = conn.getresponse()

                    if resp.status == 429:
                        resp_body = resp.read()
                        if attempt < cfg.max_retries and is_retryable_429(resp_body):
                            wait = (
                                parse_cooldown(resp_body, cfg.default_cooldown)
                                + RETRY_EXTRA_WAIT
                            )
                            attempt += 1
                            log.warning(
                                "upstream rate-limited (All models exhausted); "
                                "retry %d/%d in %ds",
                                attempt,
                                cfg.max_retries,
                                wait,
                            )
                            if attempt == 1 and cfg.notify:
                                notify_rate_limited(wait, attempt, cfg.max_retries)
                            if can_hold_stream and wait > 0:
                                self._hold_stream(wait, attempt, cfg)
                            else:
                                time.sleep(wait)
                            continue
                        self._finish(resp, preloaded=resp_body)
                        return

                    self._finish(resp)
                    return
                finally:
                    conn.close()
        except ClientGone:
            log.debug("client disconnected mid-response")
        except (OSError, http.client.HTTPException) as exc:
            if self._stream_committed:
                log.warning("upstream error mid-stream: %s", exc)
                self._send_stream_error("upstream error: %s" % exc)
            elif self._response_started:
                log.warning("upstream broke mid-response: %s", exc)
                self.close_connection = True
            else:
                log.warning("upstream error: %s", exc)
                self._send_text(502, "upstream unreachable: %s\n" % exc)

    def _read_request_body(self):
        encoding = (self.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in encoding:
            return self._read_chunked_body()
        length = self.headers.get("Content-Length")
        if not length:
            return b""
        remaining = int(length)
        chunks = []
        while remaining > 0:
            chunk = self.rfile.read(min(remaining, 65536))
            if not chunk:
                raise ValueError("client closed before sending the full body")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_chunked_body(self):
        out = bytearray()
        while True:
            line = self.rfile.readline(65536)
            if not line:
                raise ValueError("truncated chunked body")
            size = int(line.split(b";", 1)[0].strip() or b"0", 16)
            if size == 0:
                while True:
                    trailer = self.rfile.readline(65536)
                    if trailer in (b"\r\n", b"\n", b""):
                        return bytes(out)
            data = self.rfile.read(size)
            if len(data) < size:
                raise ValueError("truncated chunk")
            out += data
            self.rfile.read(2)

    def _target_path(self):
        # Origin-form is the norm; tolerate an absolute-form request target.
        if self.path.startswith(("http://", "https://")):
            parts = urlsplit(self.path)
            return parts.path + ("?" + parts.query if parts.query else "")
        return self.path

    def _forward_headers(self, upstream, body):
        headers = {}
        for key, value in self.headers.items():
            if key.lower() in HOP_BY_HOP or key.lower() == "content-length":
                continue
            headers[key] = value
        headers["Host"] = upstream.authority
        headers["Connection"] = "close"
        if body or self.headers.get("Content-Length") is not None:
            headers["Content-Length"] = str(len(body))
        return headers

    def _connect(self, upstream):
        cls = (
            http.client.HTTPSConnection
            if upstream.scheme == "https"
            else http.client.HTTPConnection
        )
        conn = cls(upstream.host, upstream.port, timeout=UPSTREAM_TIMEOUT)
        conn.connect()
        try:
            conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        return conn

    # -- response ---------------------------------------------------------

    def _relay(self, resp, preloaded=None):
        """Forward an upstream response, streaming its body as it arrives."""
        headers = [
            (key, value)
            for key, value in resp.getheaders()
            if key.lower() not in HOP_BY_HOP
        ]
        no_body = (
            self.command == "HEAD"
            or resp.status in (204, 304)
            or 100 <= resp.status < 200
        )

        if no_body:
            self._send_head(resp.status, resp.reason, headers)
            return

        if preloaded is not None:
            headers = [(k, v) for k, v in headers if k.lower() != "content-length"]
            headers.append(("Content-Length", str(len(preloaded))))
            self._send_head(resp.status, resp.reason, headers)
            self._write(preloaded)
            return

        if resp.chunked or resp.length is None:
            headers = [(k, v) for k, v in headers if k.lower() != "content-length"]
            headers.append(("Transfer-Encoding", "chunked"))
            self._send_head(resp.status, resp.reason, headers)
            self._stream_chunked(resp)
        else:
            self._send_head(resp.status, resp.reason, headers)
            self._stream_fixed(resp, resp.length)

    def _finish(self, resp, preloaded=None):
        """Deliver a terminal response, into our own stream if we opened one."""
        if not self._stream_committed:
            self._relay(resp, preloaded=preloaded)
            return
        if resp.status >= 400:
            body = preloaded if preloaded is not None else resp.read()
            detail = body.decode("utf-8", "replace").strip()
            self._send_stream_error(
                "upstream returned %d %s: %s" % (resp.status, resp.reason or "", detail)
            )
            return
        self._relay_stream_body(resp, preloaded)

    def _wants_stream(self, body):
        accept = (self.headers.get("Accept") or "").lower()
        if "text/event-stream" in accept:
            return True
        if b'"stream"' not in body:
            return False
        try:
            payload = json.loads(body)
        except ValueError:
            return False
        return isinstance(payload, dict) and payload.get("stream") is True

    def _stream_fixed(self, resp, length):
        remaining = length
        while remaining > 0:
            data = self._read1(resp, min(65536, remaining))
            if not data:
                # Upstream truncated a Content-Length body; close so the client
                # sees a short read instead of a keep-alive frame that lies.
                self.close_connection = True
                return
            self._write(data)
            remaining -= len(data)

    def _stream_chunked(self, resp):
        while True:
            data = self._read1(resp, 65536)
            if not data:
                break
            self._write(b"%x\r\n%s\r\n" % (len(data), data))
        self._write(b"0\r\n\r\n")

    @staticmethod
    def _read1(resp, size):
        try:
            return resp.read1(size)
        except AttributeError:  # pragma: no cover - very old Pythons
            return resp.read(size)

    def _hold_stream(self, wait, attempt, cfg):
        """Open the SSE response now and keep it alive so the wait looks like work."""
        if not self._stream_committed:
            # Nothing upstream has streamed yet, so committing here is safe: the
            # retry still writes the real answer into this same response.
            self._send_head(
                200,
                "OK",
                [
                    ("Content-Type", "text/event-stream; charset=utf-8"),
                    ("Cache-Control", "no-cache"),
                    ("Transfer-Encoding", "chunked"),
                ],
            )
            self._stream_committed = True

        self._comment(
            "upstream rate-limited (all models exhausted); retry %d/%d in %ds"
            % (attempt, cfg.max_retries, wait)
        )
        deadline = time.monotonic() + wait
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(cfg.keepalive_interval, remaining))
            if time.monotonic() < deadline:
                self._comment("still waiting for the upstream rate limit to clear")

    def _relay_stream_body(self, resp, preloaded=None):
        """Write just an upstream body into the SSE response we already opened."""
        if preloaded is not None:
            if preloaded:
                self._write_chunk(preloaded)
        elif resp.chunked or resp.length is None:
            while True:
                data = self._read1(resp, 65536)
                if not data:
                    break
                self._write_chunk(data)
        else:
            remaining = resp.length
            while remaining > 0:
                data = self._read1(resp, min(65536, remaining))
                if not data:
                    self.close_connection = True
                    return
                self._write_chunk(data)
                remaining -= len(data)
        self._write(b"0\r\n\r\n")

    def _send_stream_error(self, message):
        payload = json.dumps(
            {"type": "error", "error": {"type": "api_error", "message": message}}
        )
        try:
            self._write_chunk(
                b"event: error\ndata: " + payload.encode("utf-8") + b"\n\n"
            )
            self._write(b"0\r\n\r\n")
        except (ClientGone, OSError):
            self.close_connection = True

    def _comment(self, text):
        # A comment line is invisible to the SSE parser, so it cannot leak into
        # the transcript; it only proves the connection is alive.
        self._write_chunk(b": " + text.encode("utf-8") + b"\n\n")

    def _write_chunk(self, data):
        self._write(b"%x\r\n%s\r\n" % (len(data), data))

    def _send_head(self, status, reason, headers):
        self.send_response_only(status, reason)
        for key, value in headers:
            self.send_header(key, value)
        self.end_headers()
        self._response_started = True

    def _write(self, data):
        try:
            self.wfile.write(data)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
            raise ClientGone()

    def _send_text(self, status, text):
        body = text.encode("utf-8")
        try:
            self._send_head(
                status,
                None,
                [
                    ("Content-Type", "text/plain; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                    ("Connection", "close"),
                ],
            )
            self.wfile.write(body)
            self.wfile.flush()
        except OSError:
            pass
        finally:
            self.close_connection = True


# -- server ---------------------------------------------------------------

# A client that gives up before (or while) sending a request makes the socket
# raise these; socketserver would print a full traceback for each one, which is
# noise rather than a proxy failure.
CLIENT_GONE_ERRORS = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)


class ProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    cfg: Config

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, CLIENT_GONE_ERRORS):
            log.debug("client %s went away: %s", client_address[0], exc)
            return
        super().handle_error(request, client_address)


def make_server(cfg):
    server = ProxyServer((cfg.host, cfg.port), ProxyHandler)
    server.cfg = cfg
    return server


def _install_signal_handlers(server):
    def stop(*_):
        # shutdown() waits for serve_forever to return, so it cannot run on the
        # thread that is inside serve_forever.
        threading.Thread(target=server.shutdown, daemon=True).start()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, stop)
        except ValueError:  # not the main thread (tests)
            pass


def serve(cfg):
    try:
        server = make_server(cfg)
    except OSError as exc:
        raise SystemExit(
            "cannot listen on %s:%d: %s" % (cfg.host, cfg.port, exc)
        )
    _install_signal_handlers(server)
    log.info(
        "listening on http://%s:%d -> %s://%s (retries=%d, cooldown=%ds)",
        cfg.host,
        cfg.port,
        cfg.upstream.scheme,
        cfg.upstream.authority,
        cfg.max_retries,
        cfg.default_cooldown,
    )
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        log.info("stopped")


# -- self-test ------------------------------------------------------------

EXHAUSTED_429 = (
    b"API Error: Request rejected (429) - All models exhausted: 1 route checked "
    b"(1 rate-limited or on cooldown). Add more API keys or wait for rate limits "
    b"to reset. Soonest reset ~2s."
)


class _StubState:
    def __init__(self, failures):
        self.failures = failures
        self.requests = 0
        self.lock = threading.Lock()

    def hit(self):
        with self.lock:
            self.requests += 1
            return self.requests <= self.failures


def _make_stub(state):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            if state.hit():
                body = EXHAUSTED_429
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
            else:
                body = b'{"status":"ok","stub":true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def _start(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def run_selftest():
    state = _StubState(failures=1)
    stub = ThreadingHTTPServer(("127.0.0.1", 0), _make_stub(state))
    stub.daemon_threads = True
    _start(stub)

    cfg = Config(
        upstream=Upstream("http", "127.0.0.1", stub.server_address[1]),
        port=0,
        max_retries=3,
        default_cooldown=2,
    )
    proxy = make_server(cfg)
    _start(proxy)

    started = time.monotonic()
    conn = http.client.HTTPConnection("127.0.0.1", proxy.server_address[1], timeout=30)
    try:
        conn.request(
            "POST",
            "/v1/messages",
            body=b"{}",
            headers={"Content-Type": "application/json", "Content-Length": "2"},
        )
        resp = conn.getresponse()
        body = resp.read()
    finally:
        conn.close()
        proxy.shutdown()
        proxy.server_close()
        stub.shutdown()
        stub.server_close()

    elapsed = time.monotonic() - started
    ok = resp.status == 200 and b'"ok"' in body and state.requests == 2 and elapsed >= 4
    print(
        "selftest: %s (status=%d, upstream_requests=%d, elapsed=%.1fs)"
        % ("PASS" if ok else "FAIL", resp.status, state.requests, elapsed)
    )
    return 0 if ok else 1


# -- CLI ------------------------------------------------------------------


def _env_number(name, fallback, cast, kind):
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return fallback
    try:
        return cast(raw)
    except ValueError:
        raise SystemExit("%s must be %s, got %r" % (name, kind, raw))


def _env_int(name, fallback):
    return _env_number(name, fallback, int, "an integer")


def _env_float(name, fallback):
    return _env_number(name, fallback, float, "a number")


def _env_bool(name, fallback):
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return fallback
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    raise SystemExit("%s must be a boolean, got %r" % (name, raw))


def build_parser():
    parser = argparse.ArgumentParser(
        prog="freellmapi-retry",
        description=(
            "Transparent proxy that waits out FreeLLMAPI's 'All models "
            "exhausted' 429 and retries, so clients never see the error."
        ),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=_env_int("RETRY_PROXY_PORT", DEFAULT_PORT),
        help="listen port (default: %(default)s; env RETRY_PROXY_PORT)",
    )
    parser.add_argument(
        "--upstream",
        default=os.environ.get("RETRY_PROXY_UPSTREAM", DEFAULT_UPSTREAM),
        help="FreeLLMAPI base URL (default: %(default)s; env RETRY_PROXY_UPSTREAM)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=_env_int("RETRY_PROXY_MAX_RETRIES", DEFAULT_MAX_RETRIES),
        help="retries per request before returning the 429 (default: %(default)s)",
    )
    parser.add_argument(
        "--default-cooldown",
        type=int,
        default=DEFAULT_COOLDOWN,
        help="wait in seconds when the 429 omits a reset hint (default: %(default)s)",
    )
    parser.add_argument(
        "--keepalive",
        type=float,
        default=_env_float("RETRY_PROXY_KEEPALIVE", DEFAULT_KEEPALIVE),
        metavar="SECONDS",
        help=(
            "interval between SSE keepalive comments sent while waiting out a "
            "429 on a streaming request; 0 disables (default: %(default)s; "
            "env RETRY_PROXY_KEEPALIVE)"
        ),
    )
    parser.add_argument(
        "--notify",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("RETRY_PROXY_NOTIFY", True),
        help=(
            "show a macOS notification when a 429 triggers a retry; no-op on "
            "other platforms (default: %(default)s; env RETRY_PROXY_NOTIFY)"
        ),
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("RETRY_PROXY_LOG_LEVEL", "info"),
        choices=("debug", "info", "warning", "error"),
        help="logging verbosity (default: %(default)s)",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="run an in-process fake-429 test and exit",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    if args.selftest:
        return run_selftest()
    try:
        upstream = parse_upstream(args.upstream)
    except ValueError as exc:
        raise SystemExit(str(exc))
    serve(
        Config(
            upstream=upstream,
            port=args.port,
            max_retries=args.max_retries,
            default_cooldown=args.default_cooldown,
            keepalive_interval=args.keepalive,
            notify=args.notify,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
