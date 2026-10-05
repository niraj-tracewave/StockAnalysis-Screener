#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

service_pattern() {
  case "$1" in
    api) echo "uvicorn app.main:app" ;;
    market-worker) echo "celery -A app.core.celery_app:celery_app worker" ;;
    market-beat) echo "celery -A app.core.celery_app:celery_app beat" ;;
    *) return 1 ;;
  esac
}

stop_service() {
  local name="$1"
  local pidfile="$PROJECT_ROOT/run/$name.pid"
  local screen_name="stockscreener-$name"
  local stopped=0

  if screen -list 2>/dev/null | grep "[.]$screen_name[[:space:]]" >/dev/null; then
    screen -S "$screen_name" -X quit || true
    echo "Stopped $name screen session ($screen_name)"
    stopped=1
  fi

  if [ -f "$pidfile" ]; then
    local pid
    pid="$(cat "$pidfile")"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" || true
      echo "Stopped $name (PID $pid)"
      stopped=1
    fi
  fi

  while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" || true
      echo "Stopped $name (PID $pid)"
      stopped=1
    fi
  done < <(pgrep -f "$(service_pattern "$name")" || true)

  [ "$stopped" -eq 1 ] || echo "$name is not running"
  rm -f "$pidfile"
}

stop_service "market-beat"
stop_service "market-worker"
stop_service "api"
