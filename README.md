# freellmapi-retry

A tiny transparent HTTP proxy that waits out FreeLLMAPI's rate-limit 429 and retries,
so Claude Code (VS Code extension or CLI) never shows this and never needs a manual
`continue`:

```text
API Error: Request rejected (429) · All models exhausted: 1 route checked
(1 rate-limited or on cooldown). Add more API keys or wait for rate limits to reset.
Soonest reset ~89s.
```

```text
Claude Code (VS Code ext / CLI)  →  freellmapi-retry :31416  →  FreeLLMAPI :31415  →  DeepSeek
        ANTHROPIC_BASE_URL=http://127.0.0.1:31416
```

## How it works

FreeLLMAPI returns that 429 **before any model output starts** when its fallback chain
is exhausted. Because nothing has streamed to the client yet, replaying the request is
safe. The proxy:

- forwards every request verbatim and streams every response straight through
  (SSE chunks arrive live, no buffering, no added latency);
- intercepts **only** a `429` whose body contains `All models exhausted`, reads the
  `Soonest reset ~Ns` hint (`s`, `m`/`s` and decimal seconds are understood, e.g.
  `~90s`, `~4m`, `~1m30s`), waits that long plus 2 seconds, and retries the same
  request;
- while waiting, keeps a **streaming** request alive with an SSE comment every
  `--keepalive` seconds (default 10), so a ~90 s cooldown never looks like a stalled
  connection. Comments are invisible to the SSE parser, so nothing lands in the chat;
- on macOS, pops a Notification Center toast the first time a request hits the
  retryable 429, so a silent 90 s wait is visible without watching the log;
- gives up after `--max-retries` attempts (default 10) and returns the real 429, so you
  degrade to today's behaviour instead of hanging;
- serves each request on its own thread, so parallel requests don't block each other.

Anything else — other 429s, normal errors, streaming chat — passes through untouched.

## Install

Requires Python 3.9+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone <this-repo> freellmapi-retry
cd freellmapi-retry
uv tool install .
```

`uv tool install .` puts a `freellmapi-retry` executable in `~/.local/bin` (make sure
that's on your `PATH`). For development, `uv sync` then `uv run freellmapi-retry`.

## Run

Make sure FreeLLMAPI is running on its usual port, then:

```bash
freellmapi-retry
```

```text
15:50:14 INFO listening on http://127.0.0.1:31416 -> http://127.0.0.1:31415 (retries=10, cooldown=90s)
```

Check it in another terminal:

```bash
curl http://127.0.0.1:31416/api/ping        # same JSON as :31415 directly
freellmapi-retry --selftest                 # in-process fake 429 -> wait -> retry
```

## Point Claude Code at the proxy

One setting changes: the `ANTHROPIC_BASE_URL` value.

**VS Code extension.** Open the settings JSON for your active profile with
**Preferences: Open User Settings (JSON)**, then change the `claudeCode.environmentVariables`
entry for `ANTHROPIC_BASE_URL` from `http://127.0.0.1:31415` to `http://127.0.0.1:31416`:

```jsonc
"claudeCode.environmentVariables": [
    { "name": "ANTHROPIC_BASE_URL", "value": "http://127.0.0.1:31416" },
    { "name": "ANTHROPIC_AUTH_TOKEN", "value": "..." }
]
```

If you use a VS Code profile, this file is under
`~/Library/Application Support/Code/User/profiles/<id>/settings.json` rather than
`~/Library/Application Support/Code/User/settings.json`. Leave `ANTHROPIC_AUTH_TOKEN`
alone; the proxy forwards it. Reload VS Code (or restart the Claude Code session) so the
extension respawns with the new value.

**CLI / other clients.** Either per shell:

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:31416
```

or persistently in `~/.claude/settings.json`:

```json
{ "env": { "ANTHROPIC_BASE_URL": "http://127.0.0.1:31416" } }
```

Then confirm: `lsof -iTCP:31416` shows a connection to FreeLLMAPI-adjacent traffic, and a
normal message streams as before. On the next DeepSeek 429 the proxy logs
`upstream rate-limited (All models exhausted); retry 1/10 in 91s`, waits, and retries —
no error card, no `continue`.

## Configuration

| Setting | Default | Flags | Environment |
| --- | --- | --- | --- |
| listen port | `31416` | `--port` | `RETRY_PROXY_PORT` |
| upstream | `http://127.0.0.1:31415` | `--upstream` | `RETRY_PROXY_UPSTREAM` |
| max retries per request | `10` | `--max-retries` | `RETRY_PROXY_MAX_RETRIES` |
| cooldown when the 429 has no hint | `90` s | `--default-cooldown` | — |
| SSE keepalive interval while waiting | `10` s | `--keepalive` | `RETRY_PROXY_KEEPALIVE` |
| macOS notification on a 429 | on | `--notify` / `--no-notify` | `RETRY_PROXY_NOTIFY` |
| log verbosity | `info` | `--log-level {debug,info,warning,error}` | `RETRY_PROXY_LOG_LEVEL` |

Flags beat environment variables. `--version` prints the version.

## Start at login

Ready-made service files live in [contrib/](contrib/). Install the tool first, then
substitute its absolute path into the template with the one-line `sed` below.

**macOS (launchd):**

```bash
sed "s|__BIN__|$(command -v freellmapi-retry)|" contrib/com.freellmapi.retry.plist \
  > ~/Library/LaunchAgents/com.freellmapi.retry.plist
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.freellmapi.retry.plist
```

Logs go to `/tmp/freellmapi-retry.log`. Remove it with
`launchctl bootout "gui/$(id -u)" ~/Library/LaunchAgents/com.freellmapi.retry.plist`.

**Linux (systemd user service):**

```bash
mkdir -p ~/.config/systemd/user
sed "s|__BIN__|$(command -v freellmapi-retry)|" contrib/freellmapi-retry.service \
  > ~/.config/systemd/user/freellmapi-retry.service
systemctl --user daemon-reload
systemctl --user enable --now freellmapi-retry
```

Inspect with `systemctl --user status freellmapi-retry` and
`journalctl --user -u freellmapi-retry -f`.

Both services restart the proxy if it exits. If FreeLLMAPI starts after the proxy, that's
fine — the proxy connects per request, so it picks up the upstream whenever it appears.

## Testing and development

```bash
uv sync                     # create .venv and install dev tools (mypy, pylint)
./scripts/check.sh          # mypy + pylint
./scripts/check.sh --test   # mypy + pylint + unit/proxy tests

uv run python -m unittest discover -s tests -v   # unit + proxy behaviour tests
uv run freellmapi-retry --selftest               # in-process 429 -> wait -> retry
```

The tests cover verbatim request forwarding, marker detection, retry-then-success,
non-retryable 429 passthrough, the retry cap, unreachable upstreams, chunked request
bodies, incremental SSE delivery, keepalive comments, and the stream-error path.

Lint and type settings live in `pyproject.toml` (`[tool.mypy]`, `[tool.pylint.*]`).

### Pre-commit hook

`.pre-commit-config.yaml` runs the lint half of `check.sh` when a commit stages Python
changes. Install the [`pre-commit`](https://pre-commit.com) framework and enable it
once per clone:

```bash
brew install pre-commit   # or: uv tool install pre-commit
pre-commit install
```

Skip the hook for a single commit with `git commit --no-verify` (CI lints regardless).

Tests are left to CI on purpose: they sleep through real cooldowns, so they would add
about 30 seconds to every commit. GitHub Actions (`.github/workflows/ci.yml`) runs lint
on every pull request and push to `main`, and the tests plus `--selftest` on Python 3.9,
3.12, and 3.13.

## Alternative: eliminate the wait entirely

The proxy keeps you DeepSeek-only and simply waits. If you would rather **never hit the
cooldown**, give FreeLLMAPI a second route so its router fails over in milliseconds on a
DeepSeek 429 (dashboard → **Fallback** → add another model, e.g. Groq, Mistral,
OpenRouter, Google, Cerebras).

Trade-off: responses then come from a different provider, so style and limits change.
Adding more DeepSeek keys also helps without leaving DeepSeek: FreeLLMAPI rotates keys
per `(platform, model, key)` and each key has its own budget. The proxy remains useful
whenever every route in the chain is rate-limited at once.

## Notes and limits

- The proxy only helps with the *pre-stream* "All models exhausted" 429. A rate limit
  that arrives mid-stream cannot be replayed, and is passed through as-is.
- Keepalives only apply to requests that asked for a stream (`"stream": true` or
  `Accept: text/event-stream`). Once the proxy has opened that SSE response it can no
  longer answer with a `429`, so if retries run out it emits an `event: error` with the
  upstream body instead. Non-streaming clients always get the untouched status code.
- It listens on `127.0.0.1` only. Nothing is exposed to the network.
- Notifications go through `osascript` and appear under "Script Editor" in System
  Settings → Notifications; allow that app if the toast never shows. They are
  best-effort and fire-and-forget, so a missing or slow `osascript` never delays a
  retry, and `--no-notify` (or `RETRY_PROXY_NOTIFY=0`) turns them off. On Linux they
  are a no-op.
- Upstream may be `http://` or `https://`; both need no extra dependencies.
- If you change FreeLLMAPI's port, update `--upstream` (or `RETRY_PROXY_UPSTREAM`).
- Clients that connect and hang up without sending a request (browser preconnects,
  port probes) are ignored silently; they show up only with `--log-level debug`.

## License

MIT — see [LICENSE](LICENSE).
