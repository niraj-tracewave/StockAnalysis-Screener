#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

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
