#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"
exec "$PROJECT_ROOT/.venv/bin/python" -m celery \
  -A app.core.celery_app:celery_app worker \
  --loglevel=INFO \
  --queues="market-control,market-quotes,market-history,celery" \
  --concurrency="${SCREENER_WORKER_CONCURRENCY:-8}" \
  --hostname="stock-screener@%h"
