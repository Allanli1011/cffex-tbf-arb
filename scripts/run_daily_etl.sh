#!/usr/bin/env bash
# Daily ETL + signal refresh, the unit invoked by the LaunchAgent.
#
# Pipeline (sequential, ~5–7 min total wall time on a healthy connection):
#   1. backfill_market_data.py        — futures / OI rank / yield curve / repo
#   2. backfill_bond_valuation.py     — Sina single-bond YTM (1.5s/bond throttle)
#   3. compute_basis_signals.py       — per-(date,contract,bond) IRR + DV01
#   4. compute_calendar_spreads.py    — cross-quarter spreads + Z-score
#   5. compute_curve_signals.py       — fly + steepener + Z-score
#   6. compute_ctd_switch.py          — Monte-Carlo CTD switch probabilities
#   7. daily_digest.py                — actionable signal digest (md + json)
#
# A non-zero exit code propagates back to launchd so the next-run scheduler
# is unaffected; the per-step pass/fail is captured in the digest log.

set -uo pipefail   # NOT -e — we want the wrapper to keep going past failures

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="$(command -v python3 || echo /usr/bin/python3)"

LOG_DIR="$REPO_ROOT/data/logs"
mkdir -p "$LOG_DIR"

DATE_STAMP="$(date +%Y-%m-%d)"
RUN_LOG="$LOG_DIR/daily-etl-${DATE_STAMP}.log"
SUMMARY_LOG="$LOG_DIR/daily-etl-summary.log"

# Each step is (label, command). Steps share stdout/stderr; per-step exit
# codes are tracked in two parallel arrays.
STEPS_LABELS=(
  "backfill_market_data"
  "backfill_bond_valuation"
  "compute_basis_signals"
  "compute_calendar_spreads"
  "compute_curve_signals"
  "compute_ctd_switch"
  "daily_digest"
)
STEPS_CMDS=(
  "$PYTHON_BIN $REPO_ROOT/scripts/backfill_market_data.py --days 5"
  "$PYTHON_BIN $REPO_ROOT/scripts/backfill_bond_valuation.py"
  "$PYTHON_BIN $REPO_ROOT/scripts/compute_basis_signals.py"
  "$PYTHON_BIN $REPO_ROOT/scripts/compute_calendar_spreads.py --force"
  "$PYTHON_BIN $REPO_ROOT/scripts/compute_curve_signals.py"
  "$PYTHON_BIN $REPO_ROOT/scripts/compute_ctd_switch.py --n-sims 1000"
  "$PYTHON_BIN $REPO_ROOT/scripts/daily_digest.py --quiet"
)

declare -a STEP_RC=()
declare -a STEP_DT=()

run_step() {
  local label="$1"
  local cmd="$2"
  local t0 t1 rc
  t0="$(date +%s)"
  echo "[$(date '+%H:%M:%S')] >>> $label" | tee -a "$RUN_LOG"
  # shellcheck disable=SC2086
  ( cd "$REPO_ROOT" && eval $cmd ) >>"$RUN_LOG" 2>&1
  rc=$?
  t1="$(date +%s)"
  STEP_RC+=("$rc")
  STEP_DT+=("$((t1 - t0))")
  if [[ $rc -eq 0 ]]; then
    echo "[$(date '+%H:%M:%S')] ✅ $label  ($((t1 - t0))s)" | tee -a "$RUN_LOG"
  else
    echo "[$(date '+%H:%M:%S')] ❌ $label  (exit=$rc, $((t1 - t0))s)" | tee -a "$RUN_LOG"
  fi
}

START_TS="$(date +%s)"
echo "=== Daily ETL run @ $(date '+%Y-%m-%d %H:%M:%S') ===" | tee -a "$RUN_LOG"
for i in "${!STEPS_LABELS[@]}"; do
  run_step "${STEPS_LABELS[$i]}" "${STEPS_CMDS[$i]}"
done
END_TS="$(date +%s)"
TOTAL_DT=$((END_TS - START_TS))

# ---- Build summary block ------------------------------------------------
total_failures=0
for rc in "${STEP_RC[@]}"; do
  [[ "$rc" -ne 0 ]] && total_failures=$((total_failures + 1))
done

{
  echo
  echo "=== Daily ETL summary  $(date '+%Y-%m-%d %H:%M:%S') ==="
  echo "Total runtime: ${TOTAL_DT}s, failures=${total_failures}"
  for i in "${!STEPS_LABELS[@]}"; do
    if [[ "${STEP_RC[$i]}" -eq 0 ]]; then
      printf "  ✅ %-26s %3ds\n" "${STEPS_LABELS[$i]}" "${STEP_DT[$i]}"
    else
      printf "  ❌ %-26s %3ds  (exit=%s)\n" \
        "${STEPS_LABELS[$i]}" "${STEP_DT[$i]}" "${STEP_RC[$i]}"
    fi
  done

  # ---- Append the daily_digest's stdout summary ------------------------
  if [[ -f "$LOG_DIR/daily-digest-latest.json" ]]; then
    echo
    "$PYTHON_BIN" - <<EOF
import json, sys
from pathlib import Path
data = json.loads(Path("$LOG_DIR/daily-digest-latest.json").read_text())
print(f"=== Daily signal digest — {data.get('asof_overall','no data')} ===")
icon = lambda n: "🚦" if n == 0 else "🔔"
sec_top = {}
for s in data["sections"]:
    sec_top[s["name"]] = (s["rows"][0] if s["rows"] else None,
                          len(s["rows"]))
def fmt(name, sample, n):
    if not sample:
        return "—"
    if name == "basis":
        return (f"{sample['contract_id']}/{sample['bond_code']} "
                f"IRR-FDR={sample['irr_minus_fdr007_bp']:+.0f}bp")
    if name == "calendar":
        return f"{sample['product']} {sample['leg']} z={sample['z60']:+.2f}"
    if name == "curve":
        return f"{sample['structure']} z={sample['z60']:+.2f}"
    if name == "ctd_switch":
        return (f"{sample['contract_id']} {sample['switch_prob_pct']:.0f}% "
                f"→ {sample['top_alt_bond']}")
    return ""
for name in ("basis", "calendar", "curve", "ctd_switch"):
    sample, n = sec_top.get(name, (None, 0))
    print(f"  {icon(n)} {name:>12s}: {n} hits — top: {fmt(name, sample, n)}")
EOF
  fi
} | tee -a "$RUN_LOG" | tee "$SUMMARY_LOG"

echo
echo "Logs: $RUN_LOG"
echo "      $SUMMARY_LOG"
echo "Digest: $LOG_DIR/daily-digest-latest.json (+ daily-digest-<date>.{json,md})"

exit "$total_failures"
