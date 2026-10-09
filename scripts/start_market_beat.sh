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
exec "$PYTHON_BIN" -m celery \
  -A app.core.celery_app:celery_app beat \
  --loglevel=INFO \
  --schedule="$PROJECT_ROOT/celerybeat-schedule"
