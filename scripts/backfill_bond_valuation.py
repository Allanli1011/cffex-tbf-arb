"""Backfill per-bond exchange close prices and solve YTM for each
trading day a deliverable bond actually traded on the SSE.

Source: Sina (``ak.bond_zh_hs_daily``). This is a *partial* substitute
for the paid CCDC official valuation feed: coverage is good for new
benchmark bonds (~100%) but sparse for old off-the-run bonds (often
< 10%). On days a bond did not trade we have no observation and fall
back to the par-curve interpolation in ``compute_basis_signals`` —
see Phase 1.3 notes in STATUS.md.

Output: ``parquet/bond_valuation/YYYY-MM-DD.parquet`` with columns::

    date, bond_code, source ('sina_sh'), clean, ytm_pct,
    coupon_rate, maturity_date

YTM is solved from clean using
:func:`src.pricing.bond_pricing.yield_from_price`.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from loguru import logger  # noqa: E402

from src.data.fetchers import fetch_sina_bond_history  # noqa: E402
from src.data.storage import (  # noqa: E402
    PARQUET_DATASETS,
    init_schema,
    parquet_dir,
    sqlite_conn,
)
from src.data.utils import configure_logger  # noqa: E402
from src.pricing.bond_pricing import yield_from_price  # noqa: E402


SLEEP_BETWEEN_BONDS = 1.5  # seconds — Sina rate-limits bulk fetches


def _load_bond_pool() -> pd.DataFrame:
    """All bonds that are deliverable for any contract AND have an
    SSE code we can query Sina with."""
    with sqlite_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT b.bond_code, b.sh_code, b.coupon_rate,
                            b.coupon_frequency, b.maturity_date
            FROM bonds b
            INNER JOIN conversion_factors cf ON cf.bond_code = b.bond_code
            WHERE b.sh_code IS NOT NULL AND b.sh_code != ''
              AND b.coupon_rate IS NOT NULL
              AND b.maturity_date IS NOT NULL
            """,
            conn,
        )
    return df


def _solve_ytm(coupon: float, maturity: str, valuation_date: str,
               clean: float, freq: int = 1) -> float | None:
    try:
        return yield_from_price(coupon, maturity, valuation_date, clean, coupon_frequency=freq)
    except (ValueError, RuntimeError):
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill bond_valuation parquet from Sina exchange data"
    )
    parser.add_argument("--start", help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive")
    parser.add_argument("--limit", type=int,
                        help="Limit to first N bonds (for smoke testing)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing per-day files")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()
    out_dir = parquet_dir("bond_valuation")

    pool = _load_bond_pool()
    if args.limit:
        pool = pool.head(args.limit)
    logger.info(f"Pulling Sina history for {len(pool)} deliverable bonds "
                f"(throttle {SLEEP_BETWEEN_BONDS}s/bond)")

    rows: list[dict] = []
    failures = 0
    for i, b in enumerate(pool.itertuples(index=False)):
        try:
            hist = fetch_sina_bond_history(b.sh_code)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[{b.bond_code}/{b.sh_code}] failed: {exc}")
            failures += 1
            continue

        if hist.empty:
            logger.info(f"[{b.bond_code}/{b.sh_code}] no exchange history")
            continue

        if args.start:
            hist = hist[hist["date"] >= args.start]
        if args.end:
            hist = hist[hist["date"] <= args.end]

        for _, h in hist.iterrows():
            freq = int(b.coupon_frequency) if pd.notna(b.coupon_frequency) else 1
            ytm = _solve_ytm(
                float(b.coupon_rate),
                str(b.maturity_date),
                str(h["date"]),
                float(h["close"]),
                freq
            )
            if ytm is None:
                continue
            rows.append({
                "date": str(h["date"]),
                "bond_code": str(b.bond_code),
                "source": "sina_sh",
                "clean": float(h["close"]),
                "ytm_pct": float(ytm) * 100.0,
                "coupon_rate": float(b.coupon_rate),
                "maturity_date": str(b.maturity_date),
                "volume": float(h["volume"]),
            })
        logger.info(f"[{i+1}/{len(pool)}] {b.bond_code}: "
                    f"{len(hist)} trading days harvested")
        time.sleep(SLEEP_BETWEEN_BONDS)

    if not rows:
        logger.warning("No valuation rows produced.")
        return 0

    valuations = pd.DataFrame(rows).sort_values(["date", "bond_code"])
    written = 0
    skipped = 0
    for date, sub in valuations.groupby("date"):
        path = out_dir / f"{date}.parquet"
        if path.exists() and not args.force:
            skipped += 1
            continue
        sub.to_parquet(path, index=False, engine="pyarrow",
                       compression="snappy")
        written += len(sub)

    logger.success(
        f"Done. bonds={len(pool)}, rows_written={written},"
        f" skipped_existing={skipped}, failures={failures}"
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
