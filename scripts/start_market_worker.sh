#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Safely read OBJC_DISABLE_INITIALIZE_FORK_SAFETY from .env if present
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
  -A app.core.celery_app:celery_app worker \
  --loglevel=INFO \
  --queues="exchange-control,nse-quotes,bse-quotes,market-control,market-quotes,market-history,celery" \
  --concurrency="${SCREENER_WORKER_CONCURRENCY:-8}" \
  --hostname="stock-screener@%h"
