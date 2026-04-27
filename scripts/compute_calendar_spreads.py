"""Compute calendar spreads (跨期价差) across the historical window.

Reads ``parquet/futures_daily/`` and emits ``parquet/calendar_spreads/``
with columns::

    date, product, leg, near_contract, far_contract,
    near_settle, far_settle, spread, days_diff,
    z60, percentile60, z120, percentile120

Two output modes:

- ``--per-day`` (default): one parquet file per trading day, mirroring
  the rest of the data layer. Z-scores reflect the trailing window
  ending on that date.
- ``--single FILE``: emit a single concatenated parquet (handy for
  notebooks).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from loguru import logger  # noqa: E402

from src.data.storage import (  # noqa: E402
    PARQUET_DATASETS,
    init_schema,
    parquet_dir,
)
from src.data.utils import configure_logger  # noqa: E402
from src.pricing.spreads import (  # noqa: E402
    add_rolling_zscore,
    compute_spreads_for_date,
    to_dataframe,
)


def _load_futures_history() -> pd.DataFrame:
    files = sorted(PARQUET_DATASETS["futures_daily"].glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    parts = [pd.read_parquet(f) for f in files]
    return pd.concat(parts, ignore_index=True).sort_values(
        ["date", "contract_id"]
    ).reset_index(drop=True)


def _build_spreads(futures: pd.DataFrame) -> pd.DataFrame:
    """Compute spreads per date, then add rolling stats."""
    rows = []
    for date, sub in futures.groupby("date"):
        rows.extend(compute_spreads_for_date(sub))
    df = to_dataframe(rows)
    if df.empty:
        return df
    df = add_rolling_zscore(df, window=60)
    df = add_rolling_zscore(df, window=120)
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute calendar spreads")
    parser.add_argument(
        "--single", type=Path,
        help="Write a single concatenated parquet to this path",
    )
    parser.add_argument("--start", help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing per-day files")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()
    parquet_dir("calendar_spreads")

    futures = _load_futures_history()
    if futures.empty:
        logger.error("No futures_daily data found")
        return 1
    if args.start:
        futures = futures[futures["date"] >= args.start]
    if args.end:
        futures = futures[futures["date"] <= args.end]
    logger.info(f"Loaded {len(futures)} futures rows across "
                f"{futures['date'].nunique()} days")

    spreads = _build_spreads(futures)
    logger.info(f"Built {len(spreads)} spread rows across "
                f"{spreads['date'].nunique() if not spreads.empty else 0} days")

    if args.single is not None:
        args.single.parent.mkdir(parents=True, exist_ok=True)
        spreads.to_parquet(args.single, index=False)
        logger.success(f"Wrote {len(spreads)} rows -> {args.single}")
        return 0

    out_dir = parquet_dir("calendar_spreads")
    written = 0
    skipped = 0
    for date, sub in spreads.groupby("date"):
        path = out_dir / f"{date}.parquet"
        if path.exists() and not args.force:
            skipped += 1
            continue
        sub.to_parquet(path, index=False)
        written += len(sub)
    logger.success(f"Wrote {written} rows, skipped {skipped} existing days")
    return 0


if __name__ == "__main__":
    sys.exit(main())
