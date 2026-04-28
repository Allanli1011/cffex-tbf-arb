"""Compute CTD switch probabilities for every (date, contract) in the
basis_signals dataset and persist to ``parquet/ctd_switch/``.

For each trading day and listed contract we:

1. Pull the deliverable pool from ``basis_signals`` (one row per bond
   already carrying ``bond_clean``, ``modified_duration``, ``cf``)
2. Identify the current CTD (``is_ctd=True``)
3. Run a Monte-Carlo of ``--n-sims`` parallel-shift paths sized by
   ``--daily-vol-bp × √days_to_delivery``
4. Record the switch probability, top alternative, and a deterministic
   scenario table at ±25 / 50 / 100 bp shifts

Output schema::

    date, contract_id, product, current_ctd_bond, days_to_delivery,
    horizon_vol_bp, switch_probability, top_alt_bond, top_alt_prob,
    scenario_minus_100, scenario_minus_50, scenario_minus_25,
    scenario_zero, scenario_plus_25, scenario_plus_50, scenario_plus_100
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
from src.pricing.ctd_probability import (  # noqa: E402
    Deliverable,
    estimate_ctd_switch_probability,
    scenario_table,
)

DEFAULT_SHIFTS_BP = (-100, -50, -25, 25, 50, 100)


def _row(date: str, contract_id: str, product: str,
         basis_sub: pd.DataFrame,
         daily_vol_bp: float, n_sims: int, rng_seed: int) -> dict | None:
    if basis_sub.empty:
        return None
    irr_ctd_rows = basis_sub[basis_sub["is_ctd"]]
    if irr_ctd_rows.empty:
        return None
    irr_ctd_bond = str(irr_ctd_rows.iloc[0]["bond_code"])
    days = int(irr_ctd_rows.iloc[0]["n_days"])
    if days <= 0:
        return None

    # The linear MC model ranks by post-shift gross basis, so anchor on the
    # *min-gross-basis* bond at shift=0 to keep the switch probability
    # internally consistent. (basis_signals' ``is_ctd`` flag is the max-IRR
    # bond, which can differ when carry differences are material — kept as
    # ``irr_ctd_bond`` for downstream comparison.)
    gb_ctd_idx = basis_sub["gross_basis"].astype(float).idxmin()
    ctd_bond = str(basis_sub.loc[gb_ctd_idx, "bond_code"])

    pool = [
        Deliverable(
            bond_code=str(r["bond_code"]),
            clean=float(r["bond_clean"]),
            mod_dur=float(r["modified_duration"]),
            cf=float(r["cf"]),
        )
        for _, r in basis_sub.iterrows()
        if pd.notna(r["bond_clean"])
        and pd.notna(r["modified_duration"])
        and pd.notna(r["cf"])
    ]
    if len(pool) < 2:
        return None

    res = estimate_ctd_switch_probability(
        pool, ctd_bond,
        days_to_delivery=days,
        daily_vol_bp=daily_vol_bp,
        n_sims=n_sims,
        rng_seed=rng_seed,
    )
    scen = {
        f"scenario_{'plus' if bp > 0 else 'minus' if bp < 0 else 'zero'}_{abs(bp)}":
            r["ctd_bond_code"]
        for bp in DEFAULT_SHIFTS_BP
        for r in [
            next(s for s in scenario_table(pool, ctd_bond,
                                           shifts_bp=DEFAULT_SHIFTS_BP)
                 if s["shift_bp"] == bp)
        ]
    }
    out = {
        "date": date,
        "contract_id": contract_id,
        "product": product,
        "current_ctd_bond": ctd_bond,           # min-gross-basis anchor
        "irr_ctd_bond": irr_ctd_bond,           # max-IRR (basis_signals tag)
        "ctd_anchor_disagrees": ctd_bond != irr_ctd_bond,
        "days_to_delivery": days,
        "horizon_vol_bp": res.horizon_vol_bp,
        "switch_probability": res.switch_probability,
        "top_alt_bond": res.top_alternative[0] if res.top_alternative else None,
        "top_alt_prob": res.top_alternative[1] if res.top_alternative else 0.0,
    }
    out.update(scen)
    return out


def compute_for_date(date: str, *, daily_vol_bp: float,
                     n_sims: int, rng_seed: int) -> pd.DataFrame:
    bpath = PARQUET_DATASETS["basis_signals"] / f"{date}.parquet"
    if not bpath.exists():
        return pd.DataFrame()
    basis = pd.read_parquet(bpath)
    if basis.empty:
        return pd.DataFrame()

    rows: list[dict] = []
    for contract_id, sub in basis.groupby("contract_id"):
        product = str(sub["product"].iloc[0])
        r = _row(date, str(contract_id), product, sub,
                 daily_vol_bp=daily_vol_bp, n_sims=n_sims,
                 rng_seed=rng_seed)
        if r is not None:
            rows.append(r)
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute CTD switch probabilities (Monte Carlo)"
    )
    parser.add_argument("--start", help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive")
    parser.add_argument("--date", help="Single YYYY-MM-DD")
    parser.add_argument("--daily-vol-bp", type=float, default=5.0,
                        help="Daily yield vol (bp); default 5bp/day")
    parser.add_argument("--n-sims", type=int, default=1000,
                        help="Monte-Carlo paths per (date, contract)")
    parser.add_argument("--rng-seed", type=int, default=42)
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing files")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()
    out_dir = parquet_dir("ctd_switch")

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

    logger.info(f"Computing CTD switch probabilities for {len(dates)} days "
                f"(vol={args.daily_vol_bp}bp/d, n_sims={args.n_sims})")

    written = 0
    skipped = 0
    failures = 0
    for d in dates:
        try:
            out_path = out_dir / f"{d}.parquet"
            if out_path.exists() and not args.force:
                skipped += 1
                continue
            df = compute_for_date(
                d, daily_vol_bp=args.daily_vol_bp,
                n_sims=args.n_sims, rng_seed=args.rng_seed,
            )
            if df.empty:
                skipped += 1
                continue
            df.to_parquet(out_path, index=False, engine="pyarrow",
                          compression="snappy")
            written += len(df)
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"[{d}] failed: {exc}")
            failures += 1

    logger.success(
        f"Done. days={len(dates)}, rows_written={written}, "
        f"skipped={skipped}, failures={failures}"
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
