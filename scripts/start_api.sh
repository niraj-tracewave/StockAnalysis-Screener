#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  source "$PROJECT_ROOT/.env"
  set +a
fi

PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python || command -v python3)"
fi

cd "$PROJECT_ROOT"
exec "$PYTHON_BIN" -m uvicorn app.main:app \
  --host 0.0.0.0 \
  --port "${SCREENER_API_PORT:-8001}" \
  --limit-concurrency "${API_MAX_CONCURRENCY:-50}"
