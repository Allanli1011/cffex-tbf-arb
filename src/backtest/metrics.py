"""Standard backtest metrics: Sharpe, max drawdown, hit rate."""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd


TRADING_DAYS = 252


@dataclass(frozen=True)
class BacktestMetrics:
    n_trades: int
    n_winning: int
    hit_rate: float
    total_pnl: float
    avg_pnl_per_trade: float
    sharpe_annualised: float
    max_drawdown: float           # absolute RMB drawdown
    max_drawdown_pct: float       # vs peak abs(NAV)
    avg_holding_days: float


def compute_metrics(
    trades: pd.DataFrame, nav: pd.DataFrame
) -> BacktestMetrics:
    n = len(trades)
    if n == 0:
        return BacktestMetrics(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    pnls = trades["pnl_per_unit"].dropna().astype(float)
    wins = (pnls > 0).sum()
    total_pnl = float(pnls.sum())
    avg_pnl = float(pnls.mean()) if len(pnls) else 0.0
    avg_hold = float(trades["holding_days"].mean())

    sharpe = _sharpe(nav)
    dd_abs, dd_pct = _max_drawdown(nav)

    return BacktestMetrics(
        n_trades=n,
        n_winning=int(wins),
        hit_rate=float(wins / n) if n else 0.0,
        total_pnl=total_pnl,
        avg_pnl_per_trade=avg_pnl,
        sharpe_annualised=sharpe,
        max_drawdown=dd_abs,
        max_drawdown_pct=dd_pct,
        avg_holding_days=avg_hold,
    )


def _sharpe(nav: pd.DataFrame) -> float:
    if nav.empty or "daily_pnl" not in nav.columns:
        return 0.0
    daily = nav["daily_pnl"].astype(float)
    if daily.std(ddof=1) == 0 or pd.isna(daily.std(ddof=1)):
        return 0.0
    return float(daily.mean() / daily.std(ddof=1) * math.sqrt(TRADING_DAYS))


def _max_drawdown(nav: pd.DataFrame) -> tuple[float, float]:
    if nav.empty or "cum_pnl" not in nav.columns:
        return 0.0, 0.0
    series = nav["cum_pnl"].astype(float)
    peak = series.cummax()
    dd = series - peak
    dd_abs = float(dd.min())
    peak_max = float(peak.abs().max())
    pct = float(abs(dd_abs) / peak_max) if peak_max > 0 else 0.0
    return dd_abs, pct
