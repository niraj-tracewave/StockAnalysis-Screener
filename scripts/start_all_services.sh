#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  source "$PROJECT_ROOT/.env"
  set +a
fi

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY="${OBJC_DISABLE_INITIALIZE_FORK_SAFETY:-YES}"

mkdir -p "$PROJECT_ROOT/logs/services" "$PROJECT_ROOT/run"

service_pattern() {
  case "$1" in
    api) echo "uvicorn app.main:app" ;;
    market-worker) echo "celery -A app.core.celery_app:celery_app worker" ;;
    market-beat) echo "celery -A app.core.celery_app:celery_app beat" ;;
    *) return 1 ;;
  esac
}

find_service_pid() {
  pgrep -f "$(service_pattern "$1")" | head -n 1
}

screen_session_exists() {
  local listing
  listing="$(screen -list 2>/dev/null || true)"
  grep "[.]$1[[:space:]]" >/dev/null <<<"$listing"
}

start_service() {
  local name="$1"
  local script_path="$2"
  local pidfile="$PROJECT_ROOT/run/$name.pid"
  local logfile="$PROJECT_ROOT/logs/services/$name.log"
  local screen_name="stockscreener-$name"
  local running_pid=""
  local escaped_script=""
  local escaped_log=""

  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" >/dev/null 2>&1; then
    echo "$name is already running with PID $(cat "$pidfile")"
    return
  fi

  running_pid="$(find_service_pid "$name" || true)"
  if [ -n "$running_pid" ] && kill -0 "$running_pid" >/dev/null 2>&1; then
    echo "$running_pid" >"$pidfile"
    echo "$name is already running with PID $running_pid"
    return
  fi

  rm -f "$pidfile"
  if screen_session_exists "$screen_name"; then
    echo "$name is already running in screen session $screen_name"
    return
  fi

  printf -v escaped_script '%q' "$script_path"
  printf -v escaped_log '%q' "$logfile"
  screen -DmS "$screen_name" /bin/bash -c \
    "exec $escaped_script >>$escaped_log 2>&1" >/dev/null 2>&1 &
  disown || true
  sleep 2

  running_pid="$(find_service_pid "$name" || true)"
  if [ -n "$running_pid" ] && kill -0 "$running_pid" >/dev/null 2>&1; then
    echo "$running_pid" >"$pidfile"
    echo "Started $name with PID $running_pid (screen: $screen_name)"
    return
  fi

  echo "Failed to start $name. Recent log output:"
  tail -n 30 "$logfile" || true
  rm -f "$pidfile"
  return 1
}

start_service "api" "$SCRIPT_DIR/start_api.sh"
start_service "market-worker" "$SCRIPT_DIR/start_market_worker.sh"
start_service "market-beat" "$SCRIPT_DIR/start_market_beat.sh"
