#!/bin/bash
set -euo pipefail

AGENT_DIR="$HOME/Library/LaunchAgents"
UID_VALUE="$(id -u)"

for label in com.stockscreener.marketbeat com.stockscreener.marketworker com.stockscreener.api; do
  plist_path="$AGENT_DIR/$label.plist"
  launchctl bootout "gui/$UID_VALUE" "$plist_path" >/dev/null 2>&1 || true
  rm -f "$plist_path"
  echo "Removed $label"
done
