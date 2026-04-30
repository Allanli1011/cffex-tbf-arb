#!/usr/bin/env bash
# Install / uninstall macOS LaunchAgents that automate this project.
#
# Two jobs are supported:
#
#   cf-refresh   Quarterly: runs ``populate_contracts.py --snapshot`` on
#                Mar/Jun/Sep/Dec 1 at 17:00 to capture the new listed
#                contract's CFs.
#   daily-etl    Daily Mon–Fri at 17:30: runs ``run_daily_etl.sh``,
#                which sequences the full ETL + signal compute + digest
#                pipeline.
#
# Usage:
#   ./scripts/install_launchd.sh [JOB] [ACTION]
#
# JOB     = cf-refresh | daily-etl   (or empty to list both)
# ACTION  = --print (default) | --install | --uninstall | --status
#
# Defaults to dry-run (print plist) so a stray invocation never silently
# loads anything.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="$(command -v python3 || echo /usr/bin/python3)"
LOG_DIR="$REPO_ROOT/data/logs"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"

# ---- Per-job configuration ----------------------------------------------

job_label() {
  case "$1" in
    cf-refresh) echo "com.cffex.cf-refresh" ;;
    daily-etl)  echo "com.cffex.daily-etl" ;;
    *) return 1 ;;
  esac
}

job_plist_path() {
  local label
  label="$(job_label "$1")"
  echo "$LAUNCH_AGENTS_DIR/${label}.plist"
}

# Echo the schedule fragment (a sequence of <dict> entries inside a
# StartCalendarInterval array). Quarterly: 4 dates; daily-etl: 5
# weekday entries.
job_schedule_xml() {
  case "$1" in
    cf-refresh)
      cat <<EOF
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
EOF
      ;;
    daily-etl)
      # Mon=1 ... Fri=5 (Sun=0 in launchd convention)
      for wd in 1 2 3 4 5; do
        cat <<EOF
    <dict>
      <key>Weekday</key><integer>${wd}</integer>
      <key>Hour</key><integer>17</integer>
      <key>Minute</key><integer>30</integer>
    </dict>
EOF
      done
      ;;
    *) return 1 ;;
  esac
}

# Echo the ProgramArguments xml fragment.
job_program_args_xml() {
  case "$1" in
    cf-refresh)
      cat <<EOF
    <string>${PYTHON_BIN}</string>
    <string>${REPO_ROOT}/scripts/populate_contracts.py</string>
    <string>--snapshot</string>
EOF
      ;;
    daily-etl)
      cat <<EOF
    <string>/bin/bash</string>
    <string>${REPO_ROOT}/scripts/run_daily_etl.sh</string>
EOF
      ;;
    *) return 1 ;;
  esac
}

# ---- Plist generation ---------------------------------------------------

generate_plist() {
  local job="$1"
  local label
  label="$(job_label "$job")"

  cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${label}</string>

  <key>ProgramArguments</key>
  <array>
$(job_program_args_xml "$job")
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
$(job_schedule_xml "$job")
  </array>

  <key>RunAtLoad</key>
  <false/>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/${job}.out.log</string>

  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/${job}.err.log</string>
</dict>
</plist>
EOF
}

# ---- Action handlers ----------------------------------------------------

cmd_print() {
  local job="$1"
  generate_plist "$job"
  cat <<EOF >&2

# (printed plist for '${job}' above; nothing has been written)
# To install:    $0 ${job} --install
# To uninstall:  $0 ${job} --uninstall
# To check:      $0 ${job} --status
EOF
}

cmd_install() {
  local job="$1"
  local plist_path
  plist_path="$(job_plist_path "$job")"
  mkdir -p "$LOG_DIR"
  mkdir -p "$LAUNCH_AGENTS_DIR"

  if [[ -f "$plist_path" ]]; then
    echo "Existing plist at $plist_path; unloading first..." >&2
    launchctl unload "$plist_path" 2>/dev/null || true
  fi
  generate_plist "$job" > "$plist_path"
  if ! plutil -lint "$plist_path" >/dev/null; then
    echo "ERROR: generated plist is invalid" >&2
    exit 1
  fi
  launchctl load "$plist_path"
  echo "Installed and loaded $(job_label "$job")." >&2
  echo "Plist: $plist_path" >&2
  echo "Logs:  ${LOG_DIR}/${job}.{out,err}.log" >&2
  case "$job" in
    cf-refresh) echo "Next runs: Mar/Jun/Sep/Dec 1 at 17:00 local." >&2 ;;
    daily-etl)  echo "Next runs: Mon–Fri at 17:30 local." >&2 ;;
  esac
}

cmd_uninstall() {
  local job="$1"
  local plist_path
  plist_path="$(job_plist_path "$job")"
  if [[ -f "$plist_path" ]]; then
    launchctl unload "$plist_path" 2>/dev/null || true
    rm -f "$plist_path"
    echo "Removed $(job_label "$job")." >&2
  else
    echo "No plist at $plist_path; nothing to remove." >&2
  fi
}

cmd_status() {
  local job="$1"
  local plist_path label
  plist_path="$(job_plist_path "$job")"
  label="$(job_label "$job")"
  if [[ -f "$plist_path" ]]; then
    echo "Plist: $plist_path" >&2
    if launchctl list | grep -q "$label"; then
      echo "Status: loaded" >&2
      launchctl list "$label" 2>/dev/null || true
    else
      echo "Status: NOT loaded (try $0 $job --install to load)" >&2
    fi
  else
    echo "Not installed." >&2
  fi
}

list_jobs() {
  cat <<EOF
Available jobs:
  cf-refresh   Quarterly  Mar/Jun/Sep/Dec 1 17:00  populate_contracts.py --snapshot
  daily-etl    Mon–Fri    17:30                    run_daily_etl.sh

Usage:
  $0 cf-refresh             # print plist (dry run)
  $0 daily-etl --install    # install + load
  $0 daily-etl --status
  $0 cf-refresh --uninstall
EOF
}

# ---- CLI dispatch -------------------------------------------------------

JOB="${1:-}"
ACTION="${2:---print}"

case "$JOB" in
  ""|-h|--help) list_jobs ;;
  cf-refresh|daily-etl)
    case "$ACTION" in
      --print|-p)     cmd_print "$JOB" ;;
      --install|-i)   cmd_install "$JOB" ;;
      --uninstall|-u) cmd_uninstall "$JOB" ;;
      --status|-s)    cmd_status "$JOB" ;;
      *) echo "Unknown action: $ACTION" >&2; list_jobs >&2; exit 2 ;;
    esac
    ;;
  *) echo "Unknown job: $JOB" >&2; list_jobs >&2; exit 2 ;;
esac
