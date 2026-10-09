#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  VAL=$(grep -E '^OBJC_DISABLE_INITIALIZE_FORK_SAFETY=' "$PROJECT_ROOT/.env" 2>/dev/null | cut -d '=' -f2- | tr -d '"'\'' ' || true)
  if [ -n "$VAL" ]; then
    export OBJC_DISABLE_INITIALIZE_FORK_SAFETY="$VAL"
  fi
fi
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY="${OBJC_DISABLE_INITIALIZE_FORK_SAFETY:-YES}"

PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python || command -v python3)"
fi

cd "$PROJECT_ROOT"
exec "$PYTHON_BIN" -m celery \
  -A app.core.celery_app:celery_app beat \
  --loglevel=INFO \
  --schedule="$PROJECT_ROOT/celerybeat-schedule"
