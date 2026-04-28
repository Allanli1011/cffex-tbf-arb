"""Concrete strategy data loaders + run helpers.

Each ``load_*`` function returns a (date, signal, price)-shaped DataFrame
ready to feed into the engine. Each ``run_*`` function pairs a sensible
default :class:`BacktestRule` with the corresponding loader.

Two strategies live here:

- **calendar_mean_reversion** — T near-far calendar spread, z60 mean
  reversion. P&L is tracked in spread points; T futures multiplier is
  10000 RMB / point (face = 1M, quoted per 100 face).

- **basis_long_carry** — T CTD positive-carry basis. Enters when
  ``IRR - FDR007 > entry_bp``, exits as carry decays. P&L tracked via
  net basis (RMB per 100 face). Lower net basis = better for long
  basis, so engine runs with ``invert_pnl=True``. Multiplier 10000.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.storage import PARQUET_DATASETS

from .engine import BacktestRule, run_directional_carry, run_mean_reversion

T_FUTURES_MULT = 10_000.0           # RMB per 1.0 unit of price-spread move
TF_FUTURES_MULT = 10_000.0
TS_FUTURES_MULT = 20_000.0
TL_FUTURES_MULT = 10_000.0


def _load_concat(dataset: str) -> pd.DataFrame:
    files = sorted(PARQUET_DATASETS[dataset].glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    parts = [pd.read_parquet(f) for f in files]
    return pd.concat(parts, ignore_index=True)


# ---- Strategy 1: calendar mean reversion --------------------------------


def load_calendar_series(
    *, product: str = "T", leg: str = "near_far",
    z_col: str = "z60",
) -> pd.DataFrame:
    df = _load_concat("calendar_spreads")
    if df.empty:
        return pd.DataFrame(columns=["date", "signal", "price"])
    sub = df[(df["product"] == product) & (df["leg"] == leg)].copy()
    sub = sub.sort_values("date")
    sub = sub[["date", z_col, "spread"]].dropna()
    sub.columns = ["date", "signal", "price"]
    sub["signal"] = sub["signal"].astype(float)
    sub["price"] = sub["price"].astype(float)
    return sub.reset_index(drop=True)


def run_calendar_mean_reversion(
    *,
    product: str = "T",
    leg: str = "near_far",
    entry_z: float = 2.0,
    exit_z: float = 0.5,
    max_hold_days: int = 20,
    contract_size: float = T_FUTURES_MULT,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    series = load_calendar_series(product=product, leg=leg)
    rule = BacktestRule(
        entry_threshold=entry_z, exit_threshold=exit_z,
        max_hold_days=max_hold_days,
        contract_size=contract_size,
    )
    strategy = f"calendar_mr_{product}_{leg}"
    trades, nav = run_mean_reversion(series, strategy=strategy, rule=rule)
    params = {
        "loader": "calendar_spreads",
        "product": product, "leg": leg,
        "entry_z": entry_z, "exit_z": exit_z,
        "max_hold_days": max_hold_days,
        "contract_size": contract_size,
    }
    return trades, nav, params


# ---- Strategy 2: basis long-carry ---------------------------------------


def load_basis_series(*, product: str = "T") -> pd.DataFrame:
    """For each date, return the (active T contract, CTD bond) row.

    ``signal`` = ``irr_minus_fdr007_bp`` (carry over funding, bp).
    ``price``  = ``net_basis``                (RMB per 100 face).
    """
    df = _load_concat("basis_signals")
    if df.empty:
        return pd.DataFrame(columns=["date", "signal", "price"])
    sub = df[(df["product"] == product) & (df["is_ctd"])].copy()
    sub = sub.dropna(subset=["irr_minus_fdr007_bp", "net_basis"])
    if sub.empty:
        return pd.DataFrame(columns=["date", "signal", "price"])
    # If multiple contracts for the product on the same day (CTDs of
    # several listed contracts), pick the one with highest n_days
    # (typically the active month)
    sub = sub.sort_values(["date", "n_days"]).drop_duplicates(
        "date", keep="first"
    )
    sub = sub.sort_values("date")
    sub = sub[["date", "irr_minus_fdr007_bp", "net_basis"]]
    sub.columns = ["date", "signal", "price"]
    sub["signal"] = sub["signal"].astype(float)
    sub["price"] = sub["price"].astype(float)
    return sub.reset_index(drop=True)


def run_basis_long_carry(
    *,
    product: str = "T",
    entry_bp: float = 30.0,
    exit_bp: float = 5.0,
    max_hold_days: int = 30,
    contract_size: float = T_FUTURES_MULT,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    series = load_basis_series(product=product)
    # Long basis: long bond + short futures. P&L = +(net_basis_now - entry):
    # when ``net_basis`` rises from a negative entry toward zero (i.e. the
    # carry-rich position converges), the trade profits. No invert needed.
    rule = BacktestRule(
        entry_threshold=entry_bp, exit_threshold=exit_bp,
        max_hold_days=max_hold_days,
        contract_size=contract_size,
        one_sided=True, long_when="above", invert_pnl=False,
    )
    strategy = f"basis_long_carry_{product}"
    trades, nav = run_directional_carry(series, strategy=strategy, rule=rule)
    params = {
        "loader": "basis_signals",
        "product": product,
        "entry_bp": entry_bp, "exit_bp": exit_bp,
        "max_hold_days": max_hold_days,
        "contract_size": contract_size,
        "invert_pnl": False,
    }
    return trades, nav, params


STRATEGY_REGISTRY = {
    "calendar_mr_T_near_far": run_calendar_mean_reversion,
    "basis_long_carry_T": run_basis_long_carry,
}
