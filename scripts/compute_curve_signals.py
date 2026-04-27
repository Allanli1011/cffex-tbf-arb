"""Compute butterfly and steepener curve signals from basis_signals.

For each trading day we pick the most-active contract per product (max
volume from ``futures_daily``), pull the CTD's implied yield and DV01
from ``basis_signals``, then construct:

- 2-5-10 fly  (TS, TF, T)
- 5-10-30 fly (TF, T, TL)
- 2s10s steepener  (TS vs T)
- 5s30s steepener  (TF vs TL)

Each row holds DV01-neutral weights and the structure's spread in bps,
plus rolling 60-day Z-score / percentile of that spread.

Output: ``parquet/curve_signals/YYYY-MM-DD.parquet``
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
from src.pricing.curve_trades import (  # noqa: E402
    butterfly_weights,
    dv01_neutral_weights,
    fly_yield_bp,
    steepener_bp,
)

PRODUCTS = ("TS", "TF", "T", "TL")

FLY_STRUCTURES = {
    "fly_2_5_10": ("TS", "TF", "T"),
    "fly_5_10_30": ("TF", "T", "TL"),
}

STEEPENER_STRUCTURES = {
    "steepener_2s10s": ("TS", "T"),
    "steepener_5s30s": ("TF", "TL"),
}


def _load_active_per_product(date: str) -> dict[str, dict]:
    """For each product, return the contract with the highest volume on
    ``date`` along with its CTD implied yield (pct) and DV01.

    Returns ``{product: {"contract_id": str, "y_pct": float, "dv01": float}}``
    or ``{}`` if inputs missing.
    """
    fpath = PARQUET_DATASETS["futures_daily"] / f"{date}.parquet"
    bpath = PARQUET_DATASETS["basis_signals"] / f"{date}.parquet"
    if not fpath.exists() or not bpath.exists():
        return {}
    futures = pd.read_parquet(fpath)
    basis = pd.read_parquet(bpath)
    if futures.empty or basis.empty:
        return {}

    out: dict[str, dict] = {}
    for product in PRODUCTS:
        sub = futures[futures["product"] == product]
        if sub.empty:
            continue
        # Walk contracts by volume desc; first one that has a CTD wins.
        # (Some near-month contracts are in the historical CF gap and
        # carry no signals — fall through to the next-active contract
        # so we still produce a curve signal for the day.)
        sub = sub.sort_values(
            ["volume", "open_interest"], ascending=[False, False]
        )
        for contract_id in sub["contract_id"].tolist():
            ctd = basis[
                (basis["contract_id"] == contract_id) & (basis["is_ctd"])
            ]
            if ctd.empty:
                continue
            row = ctd.iloc[0]
            out[product] = {
                "contract_id": contract_id,
                "y_pct": float(row["implied_ytm"]) * 100.0,
                "dv01": float(row["futures_dv01_per_contract"]),
            }
            break
    return out


def _row_fly(date: str, name: str, legs: tuple[str, str, str],
             active: dict[str, dict]) -> dict | None:
    s, b, l = legs
    if not all(p in active for p in legs):
        return None
    a_s, a_b, a_l = active[s], active[b], active[l]
    try:
        w = butterfly_weights(a_s["dv01"], a_b["dv01"], a_l["dv01"])
    except ValueError:
        return None
    return {
        "date": date,
        "structure": name,
        "leg_short_wing": s,
        "leg_belly": b,
        "leg_long_wing": l,
        "contract_short_wing": a_s["contract_id"],
        "contract_belly": a_b["contract_id"],
        "contract_long_wing": a_l["contract_id"],
        "y_short_wing_pct": a_s["y_pct"],
        "y_belly_pct": a_b["y_pct"],
        "y_long_wing_pct": a_l["y_pct"],
        "spread_bp": fly_yield_bp(a_s["y_pct"], a_b["y_pct"], a_l["y_pct"]),
        "dv01_short_wing": a_s["dv01"],
        "dv01_belly": a_b["dv01"],
        "dv01_long_wing": a_l["dv01"],
        "n_short_wing": w.n_short_wing,
        "n_belly": w.n_belly,
        "n_long_wing": w.n_long_wing,
    }


def _row_steepener(date: str, name: str, legs: tuple[str, str],
                   active: dict[str, dict]) -> dict | None:
    s, l = legs
    if s not in active or l not in active:
        return None
    a_s, a_l = active[s], active[l]
    try:
        w = dv01_neutral_weights(a_s["dv01"], a_l["dv01"])
    except ValueError:
        return None
    return {
        "date": date,
        "structure": name,
        "leg_short_wing": s,
        "leg_belly": None,
        "leg_long_wing": l,
        "contract_short_wing": a_s["contract_id"],
        "contract_belly": None,
        "contract_long_wing": a_l["contract_id"],
        "y_short_wing_pct": a_s["y_pct"],
        "y_belly_pct": None,
        "y_long_wing_pct": a_l["y_pct"],
        "spread_bp": steepener_bp(a_s["y_pct"], a_l["y_pct"]),
        "dv01_short_wing": a_s["dv01"],
        "dv01_belly": None,
        "dv01_long_wing": a_l["dv01"],
        "n_short_wing": w.n_short,
        "n_belly": None,
        "n_long_wing": w.n_long,
    }


def compute_for_date(date: str) -> pd.DataFrame:
    active = _load_active_per_product(date)
    if not active:
        return pd.DataFrame()

    rows: list[dict] = []
    for name, legs in FLY_STRUCTURES.items():
        r = _row_fly(date, name, legs, active)
        if r is not None:
            rows.append(r)
    for name, legs in STEEPENER_STRUCTURES.items():
        r = _row_steepener(date, name, legs, active)
        if r is not None:
            rows.append(r)
    return pd.DataFrame(rows)


def add_rolling_zscore(
    df: pd.DataFrame, window: int = 60, min_periods: int = 30
) -> pd.DataFrame:
    """Add ``z<window>`` and ``percentile<window>`` per ``structure`` series
    based on the trailing window of ``spread_bp`` ordered by date.
    """
    if df.empty:
        return df
    df = df.sort_values(["structure", "date"]).reset_index(drop=True)
    z_col = f"z{window}"
    p_col = f"percentile{window}"
    df[z_col] = None
    df[p_col] = None

    for structure, idx in df.groupby("structure").groups.items():
        sub = df.loc[idx, "spread_bp"].astype(float)
        mean = sub.rolling(window, min_periods=min_periods).mean()
        std = sub.rolling(window, min_periods=min_periods).std()
        z = (sub - mean) / std
        pct = sub.rolling(window, min_periods=min_periods).apply(
            lambda s: s.rank(pct=True).iloc[-1], raw=False
        )
        df.loc[idx, z_col] = z.astype("float64")
        df.loc[idx, p_col] = pct.astype("float64")
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute curve trade signals")
    parser.add_argument("--start", help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive")
    parser.add_argument("--date", help="Single YYYY-MM-DD")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing per-day files")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()
    out_dir = parquet_dir("curve_signals")

    if args.date:
        dates = [args.date]
    elif args.start or args.end:
        s = args.start or "1900-01-01"
        e = args.end or "2999-12-31"
        files = sorted(PARQUET_DATASETS["basis_signals"].glob("*.parquet"))
        dates = [f.stem for f in files if s <= f.stem <= e]
    else:
        files = sorted(PARQUET_DATASETS["basis_signals"].glob("*.parquet"))
        dates = [f.stem for f in files]

    logger.info(f"Computing curve signals for {len(dates)} days")

    # Compute per-day, then merge for rolling stats.
    per_day: list[pd.DataFrame] = []
    skipped = 0
    failures = 0
    for d in dates:
        try:
            df = compute_for_date(d)
            if df.empty:
                skipped += 1
                continue
            per_day.append(df)
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"[{d}] failed: {exc}")
            failures += 1

    if not per_day:
        logger.warning("No curve signals produced.")
        return 0

    merged = pd.concat(per_day, ignore_index=True)
    merged = add_rolling_zscore(merged, window=60)

    written = 0
    skipped_existing = 0
    for date, sub in merged.groupby("date"):
        path = out_dir / f"{date}.parquet"
        if path.exists() and not args.force:
            skipped_existing += 1
            continue
        sub.to_parquet(path, index=False, engine="pyarrow",
                       compression="snappy")
        written += len(sub)

    logger.success(
        f"Done. days={len(dates)}, days_with_signals={merged['date'].nunique()},"
        f" rows_written={written}, skipped_inputs={skipped},"
        f" skipped_existing={skipped_existing}, failures={failures}"
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
