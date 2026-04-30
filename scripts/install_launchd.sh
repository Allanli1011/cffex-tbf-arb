#!/usr/bin/env bash
# Generate / install / uninstall the macOS LaunchAgent that runs
# ``populate_contracts.py --snapshot`` on the first day of each quarter
# at 17:00 local time.
#
# Usage:
#   ./scripts/install_launchd.sh              # print the plist to stdout (dry run)
#   ./scripts/install_launchd.sh --install    # write to ~/Library/LaunchAgents and load
#   ./scripts/install_launchd.sh --uninstall  # unload and remove the plist
#   ./scripts/install_launchd.sh --status     # show current status
#
# CFFEX publishes a fresh CF table for the next listed contract roughly
# at the start of each quarter, so we run on Mar 1 / Jun 1 / Sep 1 / Dec 1.
# If the machine is asleep at 17:00 launchd will run it on next wake.
set -euo pipefail

LABEL="com.cffex.cf-refresh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="$(command -v python3 || echo /usr/bin/python3)"
PLIST_INSTALL_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="$REPO_ROOT/data/logs"

generate_plist() {
  cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>${PYTHON_BIN}</string>
    <string>${REPO_ROOT}/scripts/populate_contracts.py</string>
    <string>--snapshot</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${REPO_ROOT}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
  </dict>

  <key>StartCalendarInterval</key>
  <array>
    <dict>
      <key>Month</key><integer>3</integer>
      <key>Day</key><integer>1</integer>
      <key>Hour</key><integer>17</integer>
      <key>Minute</key><integer>0</integer>
    </dict>
    <dict>
      <key>Month</key><integer>6</integer>
      <key>Day</key><integer>1</integer>
      <key>Hour</key><integer>17</integer>
      <key>Minute</key><integer>0</integer>
    </dict>
    <dict>
      <key>Month</key><integer>9</integer>
      <key>Day</key><integer>1</integer>
      <key>Hour</key><integer>17</integer>
      <key>Minute</key><integer>0</integer>
    </dict>
    <dict>
      <key>Month</key><integer>12</integer>
      <key>Day</key><integer>1</integer>
      <key>Hour</key><integer>17</integer>
      <key>Minute</key><integer>0</integer>
    </dict>
  </array>

  <key>RunAtLoad</key>
  <false/>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/cf-refresh.out.log</string>

  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/cf-refresh.err.log</string>
</dict>
</plist>
EOF
}

cmd_print() {
  generate_plist
  cat <<EOF >&2

# (printed plist above; nothing has been written)
# To install:    $0 --install
# To uninstall:  $0 --uninstall
# To check:      $0 --status
EOF
}

cmd_install() {
  mkdir -p "$LOG_DIR"
  mkdir -p "$(dirname "$PLIST_INSTALL_PATH")"

  if [[ -f "$PLIST_INSTALL_PATH" ]]; then
    echo "Existing plist at $PLIST_INSTALL_PATH; unloading first..." >&2
    launchctl unload "$PLIST_INSTALL_PATH" 2>/dev/null || true
  fi
  generate_plist > "$PLIST_INSTALL_PATH"
  if ! plutil -lint "$PLIST_INSTALL_PATH" >/dev/null; then
    echo "ERROR: generated plist is invalid" >&2
    exit 1
  fi
  launchctl load "$PLIST_INSTALL_PATH"
  echo "Installed and loaded ${LABEL}." >&2
  echo "Logs: ${LOG_DIR}/cf-refresh.{out,err}.log" >&2
  echo "Next runs: Mar/Jun/Sep/Dec 1 at 17:00 local." >&2
}

cmd_uninstall() {
  if [[ -f "$PLIST_INSTALL_PATH" ]]; then
    launchctl unload "$PLIST_INSTALL_PATH" 2>/dev/null || true
    rm -f "$PLIST_INSTALL_PATH"
    echo "Removed ${LABEL}." >&2
  else
    echo "No plist at $PLIST_INSTALL_PATH; nothing to remove." >&2
  fi
}

cmd_status() {
  if [[ -f "$PLIST_INSTALL_PATH" ]]; then
    echo "Plist: $PLIST_INSTALL_PATH" >&2
    if launchctl list | grep -q "$LABEL"; then
      echo "Status: loaded" >&2
      launchctl list "$LABEL" 2>/dev/null || true
    else
      echo "Status: NOT loaded (try $0 --install to load)" >&2
    fi
  else
    echo "Not installed." >&2
  fi
}

case "${1:-}" in
  ""|--print|-p) cmd_print ;;
  --install|-i)  cmd_install ;;
  --uninstall|-u) cmd_uninstall ;;
  --status|-s)   cmd_status ;;
  -h|--help)
    sed -n '1,/^set -euo pipefail/p' "$0" | sed 's/^# \{0,1\}//' | head -n -1
    ;;
  *) echo "Unknown arg: $1" >&2; exit 2 ;;
esac
