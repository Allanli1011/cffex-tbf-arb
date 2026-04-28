"""Tests for the backtest engine and metrics."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from src.backtest.engine import (
    BacktestRule,
    run_directional_carry,
    run_mean_reversion,
)
from src.backtest.metrics import compute_metrics


def _series(rows: list[tuple[str, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["date", "signal", "price"])


def test_mean_reversion_long_entry_target_exit():
    """Signal goes negative past -2, then reverts to 0 → long-spread profits
    when price rises."""
    rule = BacktestRule(entry_threshold=2.0, exit_threshold=0.5,
                        max_hold_days=20, contract_size=1.0)
    df = _series([
        ("2026-01-01", -2.5, 100.0),   # enter long-spread (signal < -entry)
        ("2026-01-02", -1.5, 100.5),
        ("2026-01-03", -0.4, 101.0),   # |signal| < exit_threshold → exit
        ("2026-01-04", 0.0, 101.5),
    ])
    trades, nav = run_mean_reversion(df, strategy="t", rule=rule)

    assert len(trades) == 1
    t = trades.iloc[0]
    assert t["direction"] == 1
    assert t["entry_date"] == "2026-01-01"
    assert t["exit_date"] == "2026-01-03"
    assert t["pnl_per_unit"] == pytest.approx(1.0)  # 101 - 100
    assert t["exit_reason"] == "target"
    # NAV should sum daily P&L correctly
    assert nav["cum_pnl"].iloc[-2] == pytest.approx(1.0)


def test_mean_reversion_short_max_hold_exit():
    """Signal stays elevated above entry — close on max_hold_days."""
    rule = BacktestRule(entry_threshold=2.0, exit_threshold=0.5,
                        max_hold_days=2, contract_size=1.0)
    df = _series([
        ("2026-01-01", 3.0, 100.0),    # short the spread
        ("2026-01-02", 2.8, 99.0),     # holding day 1
        ("2026-01-03", 2.7, 98.5),     # day 2 → max_hold reached
        ("2026-01-04", 1.0, 98.0),     # below entry, no re-entry
    ])
    trades, _ = run_mean_reversion(df, strategy="t", rule=rule)
    assert len(trades) == 1
    t = trades.iloc[0]
    assert t["direction"] == -1
    assert t["exit_reason"] == "max_hold"
    # short the spread, price dropped 1.5 → P&L = -1*(98.5-100) = +1.5
    assert t["pnl_per_unit"] == pytest.approx(1.5)


def test_directional_carry_long_when_above():
    """Long-basis carry: signal > entry → enter, signal decays → exit.
    Long the spread (price). P&L = +(price_now - price_entry) * size."""
    rule = BacktestRule(entry_threshold=30.0, exit_threshold=5.0,
                        max_hold_days=30, contract_size=10.0,
                        one_sided=True, long_when="above", invert_pnl=False)
    df = _series([
        ("2026-01-01", 40.0, -0.5),    # enter (carry > 30bp), price = net_basis
        ("2026-01-02", 25.0, -0.4),    # holding, basis converging up
        ("2026-01-03", 3.0, -0.1),     # exit (signal <= 5bp)
        ("2026-01-04", 0.0, 0.0),
    ])
    trades, nav = run_directional_carry(df, strategy="basis", rule=rule)
    assert len(trades) == 1
    t = trades.iloc[0]
    assert t["direction"] == 1
    # P&L = +1 * (-0.1 - -0.5) * 10 = 0.4 * 10 = +4
    assert t["pnl_per_unit"] == pytest.approx(4.0)
    exit_idx = nav.index[nav["date"] == "2026-01-03"][0]
    assert nav["cum_pnl"].iloc[exit_idx] == pytest.approx(4.0)


def test_directional_carry_no_entry_below_threshold():
    rule = BacktestRule(entry_threshold=30.0, exit_threshold=5.0,
                        max_hold_days=30, contract_size=1.0,
                        one_sided=True, long_when="above")
    df = _series([
        ("2026-01-01", 20.0, 0.0),
        ("2026-01-02", 25.0, 0.5),
        ("2026-01-03", 10.0, 0.2),
    ])
    trades, nav = run_directional_carry(df, strategy="t", rule=rule)
    assert len(trades) == 0
    assert nav["cum_pnl"].iloc[-1] == 0.0


def test_validation_unsorted_raises():
    rule = BacktestRule(entry_threshold=1.0, exit_threshold=0.1,
                        max_hold_days=5)
    df = _series([
        ("2026-01-02", 1.5, 1.0),
        ("2026-01-01", 1.5, 1.0),
    ])
    with pytest.raises(ValueError):
        run_mean_reversion(df, strategy="t", rule=rule)


def test_metrics_compute():
    """Synthetic 4 trades, 3 winning, mixed P&L."""
    trades = pd.DataFrame([
        {"pnl_per_unit": 100.0, "holding_days": 5},
        {"pnl_per_unit": 50.0, "holding_days": 3},
        {"pnl_per_unit": -30.0, "holding_days": 4},
        {"pnl_per_unit": 80.0, "holding_days": 6},
    ])
    nav = pd.DataFrame({
        "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
        "daily_pnl": [10.0, -5.0, 15.0],
        "cum_pnl": [10.0, 5.0, 20.0],
    })
    m = compute_metrics(trades, nav)
    assert m.n_trades == 4
    assert m.n_winning == 3
    assert m.hit_rate == pytest.approx(0.75)
    assert m.total_pnl == pytest.approx(200.0)
    assert m.avg_pnl_per_trade == pytest.approx(50.0)
    assert m.avg_holding_days == pytest.approx(4.5)
    # Drawdown: peak series = [10, 10, 20], NAV = [10, 5, 20] → min(dd) = -5
    assert m.max_drawdown == pytest.approx(-5.0)
    # max_drawdown_pct = abs(-5) / max(|peak|) = 5 / 20 = 0.25
    assert m.max_drawdown_pct == pytest.approx(0.25)
    # Sharpe finite and > 0 (positive mean daily P&L)
    assert m.sharpe_annualised > 0


def test_metrics_zero_trades():
    m = compute_metrics(pd.DataFrame(), pd.DataFrame())
    assert m.n_trades == 0
    assert m.hit_rate == 0.0
    assert m.sharpe_annualised == 0.0


# ---- Strategy registry --------------------------------------------------


def test_strategy_registry_has_all_six():
    """The registry should expose all six v1 strategies."""
    from src.backtest.strategies import STRATEGY_REGISTRY

    expected = {
        "calendar_mr_T_near_far",
        "basis_long_carry_T",
        "curve_mr_fly_2_5_10",
        "curve_mr_fly_5_10_30",
        "curve_mr_steepener_2s10s",
        "curve_mr_steepener_5s30s",
    }
    assert expected.issubset(set(STRATEGY_REGISTRY))


def test_curve_runner_unknown_structure_raises():
    """Passing an unknown structure to the curve runner should error out."""
    from src.backtest.strategies import run_curve_mean_reversion

    with pytest.raises(ValueError):
        run_curve_mean_reversion(structure="fly_made_up")


def test_curve_runner_returns_three_part_tuple():
    """Smoke test on real curve_signals data: each runner returns
    ``(trades, nav, params)`` with consistent shapes."""
    from src.backtest.strategies import (
        CURVE_CONTRACT_SIZE,
        run_curve_mean_reversion,
    )

    structure = "fly_5_10_30"
    trades, nav, params = run_curve_mean_reversion(structure=structure)
    # nav rows = number of curve_signals rows for this structure
    assert isinstance(trades, pd.DataFrame)
    assert isinstance(nav, pd.DataFrame)
    assert params["structure"] == structure
    assert params["contract_size"] == CURVE_CONTRACT_SIZE[structure]
    if not nav.empty:
        assert "cum_pnl" in nav.columns
