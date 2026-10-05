#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
AGENT_DIR="$HOME/Library/LaunchAgents"
LOG_DIR="$PROJECT_ROOT/logs/launchd"
UID_VALUE="$(id -u)"

mkdir -p "$AGENT_DIR" "$LOG_DIR"

write_plist() {
  local label="$1"
  local script_path="$2"
  local plist_path="$AGENT_DIR/$label.plist"

  cat >"$plist_path" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key><array><string>$script_path</string></array>
  <key>WorkingDirectory</key><string>$PROJECT_ROOT</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$LOG_DIR/$label.out.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/$label.err.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
EOF

  launchctl bootout "gui/$UID_VALUE" "$plist_path" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$UID_VALUE" "$plist_path"
  launchctl enable "gui/$UID_VALUE/$label" >/dev/null 2>&1 || true
  echo "Installed $label"
}

write_plist "com.stockscreener.api" "$SCRIPT_DIR/start_api.sh"
write_plist "com.stockscreener.marketworker" "$SCRIPT_DIR/start_market_worker.sh"
write_plist "com.stockscreener.marketbeat" "$SCRIPT_DIR/start_market_beat.sh"
