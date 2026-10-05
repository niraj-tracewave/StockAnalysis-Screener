#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

service_pattern() {
  case "$1" in
    api) echo "uvicorn app.main:app" ;;
    market-worker) echo "celery -A app.core.celery_app:celery_app worker" ;;
    market-beat) echo "celery -A app.core.celery_app:celery_app beat" ;;
    *) return 1 ;;
  esac
}

screen_session_exists() {
  local listing
  listing="$(screen -list 2>/dev/null || true)"
  grep "[.]$1[[:space:]]" >/dev/null <<<"$listing"
}

for service in api market-worker market-beat; do
  pidfile="$PROJECT_ROOT/run/$service.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" >/dev/null 2>&1; then
    if screen_session_exists "stockscreener-$service"; then
      echo "$service: running (PID $(cat "$pidfile"), background screen)"
    else
      echo "$service: running (PID $(cat "$pidfile"))"
    fi
  else
    running_pid="$(pgrep -f "$(service_pattern "$service")" | head -n 1 || true)"
    if [ -n "$running_pid" ] && kill -0 "$running_pid" >/dev/null 2>&1; then
      echo "$running_pid" >"$pidfile"
      if screen_session_exists "stockscreener-$service"; then
        echo "$service: running (PID $running_pid, background screen)"
      else
        echo "$service: running (PID $running_pid)"
      fi
    else
      echo "$service: stopped"
    fi
  fi
done
