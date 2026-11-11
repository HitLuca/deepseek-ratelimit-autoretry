#!/usr/bin/env bash
# Lint (and optionally test) the project.
#
#   ./scripts/check.sh          # mypy + pylint
#   ./scripts/check.sh --test   # mypy + pylint + unit tests
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv is not installed (https://docs.astral.sh/uv/)" >&2
  exit 1
fi

echo "==> mypy"
uv run mypy

echo "==> pylint"
uv run pylint src tests

if [[ "${1:-}" == "--test" || "${1:-}" == "-t" ]]; then
  echo "==> unittest"
  uv run python -m unittest discover -s tests -v
fi
