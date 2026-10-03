#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"
exec "$PROJECT_ROOT/.venv/bin/python" -m uvicorn app.main:app \
  --host 0.0.0.0 \
  --port "${SCREENER_API_PORT:-8001}" \
  --limit-concurrency "${API_MAX_CONCURRENCY:-50}"
