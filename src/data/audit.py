"""Data quality checks across all datasets.

Each check returns a :class:`CheckResult` with one of three severities:

    ``ok``      — check passed, optionally carrying summary stats
    ``warning`` — non-blocking issue worth surfacing (e.g. missing day
                  outside core coverage)
    ``error``   — blocking issue (out-of-range CF, dangling FK)

CLI runners can choose to fail loudly on ``error`` severities only.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import pandas as pd
from loguru import logger

from .cf_table import CF_MAX, CF_MIN
from .storage import PARQUET_DATASETS, sqlite_conn

Severity = str  # "ok" | "warning" | "error"


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: Severity
    message: str
    detail: dict = field(default_factory=dict)


# ---------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------


def check_sqlite_inventory() -> Iterator[CheckResult]:
    """Row counts and span for each SQLite table."""
    expected = {
        "contracts": "contract_id",
        "bonds": "bond_code",
        "conversion_factors": "contract_id",
        "signals": "signal_date",
        "etl_runs": "run_date",
    }
    with sqlite_conn() as conn:
        for table, span_col in expected.items():
            try:
                row = conn.execute(
                    f"SELECT COUNT(*), MIN({span_col}), MAX({span_col}) "
                    f"FROM {table}"
                ).fetchone()
            except Exception as exc:  # noqa: BLE001
                yield CheckResult(
                    f"sqlite.{table}",
                    "error",
                    f"table query failed: {exc}",
                )
                continue
            n, lo, hi = row
            yield CheckResult(
                f"sqlite.{table}",
                "ok",
                f"{n} rows, span {lo}..{hi}" if n else "empty",
                {"rows": n, "span_min": lo, "span_max": hi},
            )


def check_parquet_inventory() -> Iterator[CheckResult]:
    """Per-dataset file count and date range."""
    for ds, path in PARQUET_DATASETS.items():
        if not path.exists():
            yield CheckResult(f"parquet.{ds}", "warning", "directory missing")
            continue
        files = sorted(path.glob("*.parquet"))
        if not files:
            yield CheckResult(f"parquet.{ds}", "warning", "no files")
            continue
        # File-name-based date span (cheap)
        dates = [f.stem for f in files if _is_iso_date(f.stem)]
        span = (min(dates), max(dates)) if dates else ("?", "?")
        yield CheckResult(
            f"parquet.{ds}",
            "ok",
            f"{len(files)} files, {span[0]}..{span[1]}",
            {
                "file_count": len(files),
                "date_min": span[0],
                "date_max": span[1],
            },
        )


# ---------------------------------------------------------------------
# Consistency (cross-table references)
# ---------------------------------------------------------------------


def check_cf_bond_consistency() -> Iterator[CheckResult]:
    """Every (contract_id, bond_code) in conversion_factors should resolve
    to a row in ``bonds`` and a row in ``contracts``.
    """
    with sqlite_conn() as conn:
        orphan_bonds = conn.execute(
            """SELECT COUNT(*) FROM conversion_factors cf
               LEFT JOIN bonds b ON cf.bond_code = b.bond_code
               WHERE b.bond_code IS NULL"""
        ).fetchone()[0]
        orphan_contracts = conn.execute(
            """SELECT COUNT(*) FROM conversion_factors cf
               LEFT JOIN contracts c ON cf.contract_id = c.contract_id
               WHERE c.contract_id IS NULL"""
        ).fetchone()[0]

    yield CheckResult(
        "consistency.cf_bond_fk",
        "error" if orphan_bonds else "ok",
        f"{orphan_bonds} CF rows reference missing bond_code"
        if orphan_bonds
        else "all CF rows have a matching bond",
        {"orphan_count": orphan_bonds},
    )
    yield CheckResult(
        "consistency.cf_contract_fk",
        "error" if orphan_contracts else "ok",
        f"{orphan_contracts} CF rows reference missing contract_id"
        if orphan_contracts
        else "all CF rows have a matching contract",
        {"orphan_count": orphan_contracts},
    )


def check_bonds_completeness() -> Iterator[CheckResult]:
    """Bonds with NULL coupon or maturity are unusable for CF/IRR."""
    with sqlite_conn() as conn:
        missing_coupon = conn.execute(
            "SELECT COUNT(*) FROM bonds WHERE coupon_rate IS NULL"
        ).fetchone()[0]
        missing_maturity = conn.execute(
            "SELECT COUNT(*) FROM bonds WHERE maturity_date IS NULL "
            "OR maturity_date = ''"
        ).fetchone()[0]

    yield CheckResult(
        "completeness.bonds_coupon",
        "warning" if missing_coupon else "ok",
        f"{missing_coupon} bonds missing coupon_rate"
        if missing_coupon
        else "all bonds have coupon_rate",
        {"missing": missing_coupon},
    )
    yield CheckResult(
        "completeness.bonds_maturity",
        "warning" if missing_maturity else "ok",
        f"{missing_maturity} bonds missing maturity_date"
        if missing_maturity
        else "all bonds have maturity_date",
        {"missing": missing_maturity},
    )


# ---------------------------------------------------------------------
# Range / sanity checks
# ---------------------------------------------------------------------


def check_cf_range() -> Iterator[CheckResult]:
    """All CF values must lie in the configured sanity bounds."""
    with sqlite_conn() as conn:
        out = conn.execute(
            "SELECT contract_id, bond_code, cf FROM conversion_factors "
            "WHERE cf < ? OR cf > ?",
            (CF_MIN, CF_MAX),
        ).fetchall()
    yield CheckResult(
        "range.cf_bounds",
        "error" if out else "ok",
        f"{len(out)} CFs outside [{CF_MIN}, {CF_MAX}]"
        if out
        else f"all CFs in [{CF_MIN}, {CF_MAX}]",
        {"violations": [tuple(r) for r in out[:20]]},
    )


def check_futures_price_sanity() -> Iterator[CheckResult]:
    """Futures close should be > 0 and within plausible TBF range."""
    files = sorted(PARQUET_DATASETS["futures_daily"].glob("*.parquet"))
    if not files:
        yield CheckResult("range.futures_price", "warning", "no parquet files")
        return

    bad_rows: list[dict] = []
    skipped_files: list[str] = []
    for f in files:
        df = pd.read_parquet(f)
        if "close" not in df.columns:
            skipped_files.append(f.name)
            continue
        mask = (df["close"] <= 0) | (df["close"].isna()) | \
               (df["close"] < 80) | (df["close"] > 200)
        if mask.any():
            bad_rows.extend(df[mask].to_dict("records"))

    if skipped_files:
        yield CheckResult(
            "range.futures_price.schema",
            "warning",
            f"{len(skipped_files)} files skipped (missing 'close' column)",
            {"files": skipped_files[:10]},
        )

    yield CheckResult(
        "range.futures_price",
        "warning" if bad_rows else "ok",
        f"{len(bad_rows)} rows with close outside (0, 80..200]"
        if bad_rows
        else "all futures closes in plausible range",
        {"bad_rows_sample": bad_rows[:5]},
    )


def check_yield_curve_sanity() -> Iterator[CheckResult]:
    """Curve yields should be positive and < 20%."""
    files = sorted(PARQUET_DATASETS["bond_yield_curve"].glob("*.parquet"))
    if not files:
        yield CheckResult("range.yield_curve", "warning", "no parquet files")
        return
    bad: list[dict] = []
    for f in files:
        df = pd.read_parquet(f)
        mask = (df["yield_pct"] <= 0) | (df["yield_pct"] > 20)
        if mask.any():
            bad.extend(df[mask].to_dict("records"))
    yield CheckResult(
        "range.yield_curve",
        "warning" if bad else "ok",
        f"{len(bad)} curve rows outside (0, 20]"
        if bad
        else "all curve points in plausible range",
        {"bad_rows_sample": bad[:5]},
    )


# ---------------------------------------------------------------------
# Calendar gaps
# ---------------------------------------------------------------------


def check_trading_day_gaps(dataset: str = "futures_daily",
                           lookback_days: int = 60
                           ) -> Iterator[CheckResult]:
    """Within the last ``lookback_days`` trading days, every day should
    have a corresponding parquet file. Missing days are reported as
    warnings.
    """
    from .calendar import latest_trading_day, load_calendar

    if dataset not in PARQUET_DATASETS:
        yield CheckResult(
            f"gaps.{dataset}", "error", f"unknown dataset {dataset!r}"
        )
        return

    today = latest_trading_day()
    cal = load_calendar()
    expected = sorted(
        d.isoformat() for d in cal["date"].tolist()
        if d <= today and (today - d).days <= lookback_days
    )
    expected = expected[-lookback_days:]
    actual = {f.stem for f in PARQUET_DATASETS[dataset].glob("*.parquet")}
    missing = sorted(set(expected) - actual)
    yield CheckResult(
        f"gaps.{dataset}",
        "warning" if missing else "ok",
        f"{len(missing)} missing days in last {lookback_days} trading days"
        if missing
        else f"complete coverage of last {lookback_days} trading days",
        {
            "missing": missing,
            "expected_count": len(expected),
            "actual_count": len(actual & set(expected)),
        },
    )


# ---------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------


CHECKS = [
    check_sqlite_inventory,
    check_parquet_inventory,
    check_cf_bond_consistency,
    check_bonds_completeness,
    check_cf_range,
    check_futures_price_sanity,
    check_yield_curve_sanity,
    lambda: check_trading_day_gaps("futures_daily", 60),
    lambda: check_trading_day_gaps("bond_yield_curve", 60),
]


def run_all_checks() -> list[CheckResult]:
    out: list[CheckResult] = []
    for fn in CHECKS:
        try:
            out.extend(fn())
        except Exception as exc:  # noqa: BLE001
            out.append(
                CheckResult(
                    fn.__name__ if hasattr(fn, "__name__") else "anonymous",
                    "error",
                    f"check raised: {exc}",
                )
            )
    return out


def summarise(results: list[CheckResult]) -> dict[str, int]:
    return {
        "ok": sum(r.severity == "ok" for r in results),
        "warning": sum(r.severity == "warning" for r in results),
        "error": sum(r.severity == "error" for r in results),
    }


# ---------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------


_SEV_BADGE = {"ok": "✅", "warning": "⚠️", "error": "❌"}


def render_markdown(results: list[CheckResult]) -> str:
    """Render an audit report as a single Markdown string."""
    summary = summarise(results)
    lines: list[str] = []
    lines.append("# Data Audit Report")
    lines.append("")
    lines.append(f"_Generated: {dt.datetime.now().isoformat(timespec='seconds')}_")
    lines.append("")
    lines.append(
        f"**Summary**: {summary['ok']} ok, "
        f"{summary['warning']} warning, "
        f"{summary['error']} error"
    )
    lines.append("")
    lines.append("| | Check | Message |")
    lines.append("|--|--|--|")
    for r in results:
        badge = _SEV_BADGE.get(r.severity, "?")
        lines.append(f"| {badge} | `{r.name}` | {r.message} |")
    # Detail dump for non-ok rows
    nonok = [r for r in results if r.severity != "ok"]
    if nonok:
        lines.append("")
        lines.append("## Details")
        for r in nonok:
            lines.append(f"### `{r.name}` — {r.severity}")
            lines.append(f"- Message: {r.message}")
            if r.detail:
                lines.append(f"- Detail: `{r.detail}`")
    return "\n".join(lines) + "\n"


def render_json(results: list[CheckResult]) -> str:
    """JSON dump for machine consumption."""
    import json
    return json.dumps(
        {
            "summary": summarise(results),
            "checks": [
                {
                    "name": r.name,
                    "severity": r.severity,
                    "message": r.message,
                    "detail": r.detail,
                }
                for r in results
            ],
        },
        ensure_ascii=False,
        indent=2,
    )


def _is_iso_date(s: str) -> bool:
    try:
        dt.date.fromisoformat(s)
        return True
    except ValueError:
        return False
