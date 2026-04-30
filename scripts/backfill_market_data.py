"""Backfill daily futures + OI rank data over a date range.

Examples::

    # Refresh today
    python3 scripts/backfill_market_data.py

    # Refresh specific date
    python3 scripts/backfill_market_data.py --date 2026-04-24

    # Backfill last 30 trading days
    python3 scripts/backfill_market_data.py --days 30

    # Backfill an explicit range (inclusive)
    python3 scripts/backfill_market_data.py --start 2025-01-02 --end 2026-04-24

The fetcher only writes files that don't already exist (idempotent).
Use ``--force`` to overwrite.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from loguru import logger  # noqa: E402

from src.data.calendar import (  # noqa: E402
    is_trading_day,
    latest_trading_day,
    trading_days_between,
)
from src.data.fetchers import (  # noqa: E402
    GC_SYMBOLS,
    fetch_cfets_repo_fixings,
    fetch_cffex_daily,
    fetch_cffex_oi_rank,
    fetch_exchange_repo,
    fetch_shibor,
    fetch_treasury_yield_curve,
)
from src.data.storage import init_schema, parquet_dir  # noqa: E402
from src.data.utils import configure_logger  # noqa: E402


def _save_parquet(df, dataset: str, run_date: str, force: bool) -> int:
    out = parquet_dir(dataset) / f"{run_date}.parquet"
    if out.exists() and not force:
        return -1  # signals "skipped"
    df.to_parquet(out, index=False, engine="pyarrow", compression="snappy")
    return len(df)


def _process_day(date: dt.date, force: bool) -> dict[str, int]:
    iso = date.isoformat()
    summary = {}

    try:
        df = fetch_cffex_daily(date)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"[{iso}] fetch_cffex_daily failed: {exc}")
        df = None

    if df is not None:
        if df.empty:
            logger.warning(f"[{iso}] futures_daily: empty (likely non-trading day)")
            summary["futures_daily"] = 0
        else:
            n = _save_parquet(df, "futures_daily", iso, force)
            if n == -1:
                logger.info(f"[{iso}] futures_daily: skipped (file exists)")
            else:
                logger.info(f"[{iso}] futures_daily: {n} rows")
            summary["futures_daily"] = max(n, 0)

    try:
        rank = fetch_cffex_oi_rank(date)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"[{iso}] fetch_cffex_oi_rank failed: {exc}")
        rank = None

    if rank is not None:
        if rank.empty:
            summary["futures_oi_rank"] = 0
        else:
            n = _save_parquet(rank, "futures_oi_rank", iso, force)
            if n == -1:
                logger.info(f"[{iso}] futures_oi_rank: skipped (file exists)")
            else:
                logger.info(f"[{iso}] futures_oi_rank: {n} rows")
            summary["futures_oi_rank"] = max(n, 0)

    return summary


def _process_yield_curve(start: dt.date, end: dt.date, force: bool) -> int:
    """CCDC range API requires window < 1 year; we chunk by 330 days."""
    parts: list[pd.DataFrame] = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + dt.timedelta(days=330), end)
        try:
            df = fetch_treasury_yield_curve(cur.isoformat(), chunk_end.isoformat())
            if not df.empty:
                parts.append(df)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                f"yield_curve fetch {cur}..{chunk_end} failed: {exc}"
            )
        cur = chunk_end + dt.timedelta(days=1)

    if not parts:
        logger.warning("yield_curve: no rows in range")
        return 0

    combined = pd.concat(parts, ignore_index=True).drop_duplicates(
        subset=["date", "tenor_years"]
    )
    n_total = 0
    for d, sub in combined.groupby("date"):
        n = _save_parquet(sub, "bond_yield_curve", d, force)
        if n != -1:
            n_total += n
    if n_total:
        logger.info(
            f"yield_curve: {n_total} rows across "
            f"{combined['date'].nunique()} days"
        )
    return n_total


def _process_funding_rates(start: dt.date, end: dt.date, force: bool) -> int:
    """Pull all funding rates and save one parquet per date.

    CFETS fixings are fetched per-month (API constraint stays loose but we
    chunk to be safe). Shibor + GC are full-history single-call APIs that
    we slice client-side.
    """
    parts: list = []

    # CFETS by month chunks
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        # End of month
        next_month = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        chunk_end = min(next_month - dt.timedelta(days=1), end)
        chunk_start = max(cur, start)
        try:
            df = fetch_cfets_repo_fixings(
                chunk_start.isoformat(), chunk_end.isoformat()
            )
            if not df.empty:
                parts.append(df)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"CFETS fixings {chunk_start}..{chunk_end} failed: {exc}")
        cur = next_month

    # Exchange repo (GC001/007/014). Sina is the source of truth here;
    # the original eastmoney path was flaky through proxies. Brief
    # inter-symbol sleep to stay polite.
    for i, sym in enumerate(GC_SYMBOLS):
        if i > 0:
            time.sleep(2.0)
        try:
            df = fetch_exchange_repo(symbol=sym)
            df = df[(df["date"] >= start.isoformat())
                    & (df["date"] <= end.isoformat())]
            if not df.empty:
                parts.append(df)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"exchange repo {sym} failed: {exc}")

    # Shibor
    try:
        df = fetch_shibor()
        df = df[(df["date"] >= start.isoformat())
                & (df["date"] <= end.isoformat())]
        if not df.empty:
            parts.append(df)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"shibor failed: {exc}")

    if not parts:
        logger.warning("funding rates: no rows in range")
        return 0

    combined = pd.concat(parts, ignore_index=True)
    n_total = 0
    for d, sub in combined.groupby("date"):
        n = _save_parquet(sub, "repo_rate", d, force)
        if n != -1:
            n_total += n
    if n_total:
        logger.info(
            f"funding rates: {n_total} rows across "
            f"{combined['date'].nunique()} days, "
            f"{combined['rate_name'].nunique()} rate series"
        )
    return n_total


def _resolve_dates(args) -> list[dt.date]:
    if args.date:
        d = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
        if not is_trading_day(d):
            logger.warning(f"{d} is not a trading day; rolling back")
            from src.data.calendar import previous_trading_day
            d = previous_trading_day(d)
        return [d]

    if args.start and args.end:
        s = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
        e = dt.datetime.strptime(args.end, "%Y-%m-%d").date()
        return trading_days_between(s, e)

    if args.days:
        end = latest_trading_day()
        # walk backwards N trading days
        from src.data.calendar import load_calendar
        cal = load_calendar()
        eligible = cal[cal["date"] <= end]["date"].tolist()
        return eligible[-args.days :]

    # default: just today's most recent trading day
    return [latest_trading_day()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill CFFEX market data")
    parser.add_argument("--date", help="Single trading day YYYY-MM-DD")
    parser.add_argument("--start", help="Range start YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", help="Range end YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--days", type=int, help="Last N trading days (counting from today)"
    )
    parser.add_argument(
        "--force", action="store_true", help="Overwrite existing parquet files"
    )
    parser.add_argument(
        "--skip-yield-curve", action="store_true",
        help="Skip CCDC yield curve refresh (it covers the whole range in one call)",
    )
    parser.add_argument(
        "--skip-funding-rates", action="store_true",
        help="Skip CFETS / GC / Shibor refresh",
    )
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    dates = _resolve_dates(args)
    if not dates:
        logger.error("No trading days resolved")
        return 1

    logger.info(f"Processing {len(dates)} trading days: "
                f"{dates[0]} .. {dates[-1]}")

    totals = {
        "futures_daily": 0,
        "futures_oi_rank": 0,
        "bond_yield_curve": 0,
        "repo_rate": 0,
    }
    failures = 0
    for d in dates:
        try:
            s = _process_day(d, args.force)
            for k, v in s.items():
                totals[k] = totals.get(k, 0) + max(v, 0)
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"[{d}] unexpected failure: {exc}")
            failures += 1

    # Yield curve fetched once over the whole range (range API)
    if not args.skip_yield_curve and len(dates) > 0:
        try:
            n = _process_yield_curve(dates[0], dates[-1], args.force)
            if n > 0:
                totals["bond_yield_curve"] = n
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"yield_curve unexpected failure: {exc}")
            failures += 1

    # Funding rates (CFETS fixings + GC + Shibor) fetched once per range
    if not args.skip_funding_rates and len(dates) > 0:
        try:
            n = _process_funding_rates(dates[0], dates[-1], args.force)
            if n > 0:
                totals["repo_rate"] = n
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"funding_rates unexpected failure: {exc}")
            failures += 1

    logger.success(
        f"Done. totals={totals}, days={len(dates)}, failures={failures}"
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
