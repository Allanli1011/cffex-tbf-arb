"""Compute basis / net-basis / IRR signals for every (date, contract, bond)
across the historical window and persist to parquet.

Pipeline (per trading day):
1. Load futures_daily (settle per contract)
2. Load bond_yield_curve (par yields by tenor)
3. Join contracts ↔ deliverable bonds via SQLite ``conversion_factors``
4. For each deliverable bond:
   a) Interpolate YTM at the bond's remaining tenor
   b) Price the bond from YTM (clean + accrued)
   c) Compute basis / net basis / IRR with delivery = 2nd Friday of
      contract month (CFFEX convention; close enough — operators can
      override later)
5. Write parquet/basis_signals/YYYY-MM-DD.parquet

Outputs columns::

    date, contract_id, product, bond_code, bond_name, coupon, maturity,
    cf, futures_settle, ytm_used, bond_clean, accrued_now, accrued_T,
    invoice, gross_basis, net_basis, irr, irr_minus_fdr007_bp,
    days_to_delivery, is_ctd
"""

from __future__ import annotations

import argparse
import datetime as dt
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
    sqlite_conn,
)
from src.data.utils import configure_logger  # noqa: E402
from src.pricing.bond_pricing import (  # noqa: E402
    futures_dv01,
    implied_ytm_from_futures,
    interpolate_yield,
    price_from_yield,
)
from src.pricing.cf_calculator import parse_contract_id  # noqa: E402
from src.pricing.irr import compute_basis, irr_minus_repo_bp  # noqa: E402


def _second_friday(year: int, month: int) -> dt.date:
    """CFFEX TBF actual delivery date = 2nd Friday of contract month."""
    d = dt.date(year, month, 1)
    # weekday(): Monday=0 ... Friday=4
    offset = (4 - d.weekday()) % 7
    first_friday = d + dt.timedelta(days=offset)
    return first_friday + dt.timedelta(days=7)


def _delivery_date_for(contract_id: str) -> dt.date:
    """Return the actual delivery date (2nd Friday of contract month)."""
    _, month_start = parse_contract_id(contract_id)
    return _second_friday(month_start.year, month_start.month)


def _load_inputs(date: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, float | None]:
    """Return (futures_daily, yield_curve, cf_join_bonds, fdr007_pct) for one date."""
    fpath = PARQUET_DATASETS["futures_daily"] / f"{date}.parquet"
    cpath = PARQUET_DATASETS["bond_yield_curve"] / f"{date}.parquet"
    rpath = PARQUET_DATASETS["repo_rate"] / f"{date}.parquet"

    if not fpath.exists() or not cpath.exists():
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None

    futures = pd.read_parquet(fpath)
    curve = pd.read_parquet(cpath)
    repo = pd.read_parquet(rpath) if rpath.exists() else pd.DataFrame()

    fdr007 = None
    if not repo.empty:
        m = repo[repo["rate_name"] == "FDR007"]
        if not m.empty:
            fdr007 = float(m["value_pct"].iloc[0])

    with sqlite_conn() as conn:
        cfs = pd.read_sql_query(
            """
            SELECT cf.contract_id, cf.bond_code, cf.cf,
                   b.bond_name, b.coupon_rate, b.maturity_date
            FROM conversion_factors cf
            LEFT JOIN bonds b ON cf.bond_code = b.bond_code
            """,
            conn,
        )

    return futures, curve, cfs, fdr007


def _compute_for_date(date: str) -> pd.DataFrame:
    valuation = dt.date.fromisoformat(date)
    futures, curve, cfs, fdr007 = _load_inputs(date)
    if futures.empty or curve.empty or cfs.empty:
        return pd.DataFrame()

    # Yield curve as parallel arrays for interpolation
    tenors = curve["tenor_years"].astype(float).tolist()
    yields_pct = curve["yield_pct"].astype(float).tolist()

    # Index futures by contract
    f_by_contract = dict(zip(futures["contract_id"], futures["settle"]))

    rows: list[dict] = []
    for r in cfs.itertuples(index=False):
        if r.contract_id not in f_by_contract:
            continue
        if pd.isna(r.coupon_rate) or pd.isna(r.maturity_date) or not r.maturity_date:
            continue
        try:
            maturity = dt.date.fromisoformat(r.maturity_date)
        except ValueError:
            continue
        if maturity <= valuation:
            continue

        try:
            delivery = _delivery_date_for(r.contract_id)
        except ValueError:
            continue
        if delivery <= valuation or delivery > maturity:
            continue

        # Interpolate yield at the bond's remaining tenor (years from valuation)
        bond_tenor = (maturity - valuation).days / 365.0
        ytm_pct = interpolate_yield(tenors, yields_pct, bond_tenor)
        ytm = ytm_pct / 100.0

        product = (r.contract_id[:2] if r.contract_id[:2] in {"TS", "TF", "TL"}
                   else r.contract_id[0])
        try:
            pricing = price_from_yield(
                float(r.coupon_rate), maturity, valuation, ytm
            )
            futures_settle = float(f_by_contract[r.contract_id])
            quote = compute_basis(
                valuation_date=valuation,
                delivery_date=delivery,
                bond_clean=pricing.clean,
                coupon_rate=float(r.coupon_rate),
                maturity=maturity,
                futures=futures_settle,
                cf=float(r.cf),
            )
            implied_ytm = implied_ytm_from_futures(
                futures_price=futures_settle,
                cf=float(r.cf),
                coupon_rate=float(r.coupon_rate),
                maturity=maturity,
                valuation_date=valuation,
            )
            dv01 = futures_dv01(
                futures_price=futures_settle,
                cf=float(r.cf),
                coupon_rate=float(r.coupon_rate),
                maturity=maturity,
                valuation_date=valuation,
                product=product,
                implied_ytm=implied_ytm,
            )
        except Exception:  # noqa: BLE001 - skip bad rows, keep going
            continue

        rows.append({
            "date": date,
            "contract_id": r.contract_id,
            "product": product,
            "bond_code": r.bond_code,
            "bond_name": r.bond_name,
            "coupon_rate": float(r.coupon_rate),
            "maturity_date": r.maturity_date,
            "cf": float(r.cf),
            "futures_settle": futures_settle,
            "ytm_used": ytm,
            "implied_ytm": implied_ytm,
            "bond_clean": pricing.clean,
            "modified_duration": pricing.modified_dur,
            "futures_dv01_per_contract": dv01.dv01_per_contract,
            "accrued_now": quote.accrued_now,
            "accrued_at_delivery": quote.accrued_at_delivery,
            "coupons_during": quote.coupons_during,
            "invoice_price": quote.invoice_price,
            "gross_basis": quote.gross_basis,
            "carry": quote.carry,
            "net_basis": quote.net_basis,
            "irr": quote.irr_annualised,
            "n_days": quote.n_days,
            "irr_minus_fdr007_bp": (
                irr_minus_repo_bp(quote.irr_annualised, fdr007)
                if fdr007 is not None else None
            ),
        })

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)

    # Mark CTD: per (date, contract_id), highest IRR
    df["is_ctd"] = False
    for cid, sub in df.groupby("contract_id"):
        idx = sub["irr"].idxmax()
        df.loc[idx, "is_ctd"] = True
    return df


def _save(df: pd.DataFrame, date: str, force: bool) -> int:
    out = parquet_dir("basis_signals") / f"{date}.parquet"
    if out.exists() and not force:
        return -1
    df.to_parquet(out, index=False, engine="pyarrow", compression="snappy")
    return len(df)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute basis/IRR signals")
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--date", help="Single YYYY-MM-DD")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing files")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()
    parquet_dir("basis_signals")  # ensure dir exists

    # Default: every date that has both futures_daily and bond_yield_curve
    if args.date:
        dates = [args.date]
    elif args.start or args.end:
        s = args.start or "1900-01-01"
        e = args.end or "2999-12-31"
        files = sorted(PARQUET_DATASETS["futures_daily"].glob("*.parquet"))
        dates = [f.stem for f in files if s <= f.stem <= e]
    else:
        files = sorted(PARQUET_DATASETS["futures_daily"].glob("*.parquet"))
        dates = [f.stem for f in files]

    logger.info(f"Computing signals for {len(dates)} days")
    total_rows = 0
    skipped = 0
    failures = 0
    for d in dates:
        try:
            df = _compute_for_date(d)
            if df.empty:
                skipped += 1
                continue
            n = _save(df, d, args.force)
            if n == -1:
                continue
            total_rows += n
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"[{d}] failed: {exc}")
            failures += 1

    logger.success(
        f"Done. days={len(dates)}, rows_written={total_rows}, "
        f"skipped={skipped}, failures={failures}"
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
