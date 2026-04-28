"""Single-instrument event-driven backtest engine.

The engine consumes a date-sorted DataFrame with three required columns:

- ``date``    — string YYYY-MM-DD, monotonically increasing
- ``signal``  — the metric used to trigger entry/exit (e.g. z-score, bp)
- ``price``   — the spread / basis level used to mark P&L (one number per day)

Two trade-management modes are provided:

1. :func:`run_mean_reversion` — symmetric long/short rule keyed on a
   z-score-style signal. Entry when ``|signal| > entry_threshold``,
   exit when ``|signal| < exit_threshold`` or after ``max_hold_days``.
   Direction is the opposite sign of the entry signal (we expect
   reversion to zero).

2. :func:`run_directional_carry` — one-sided rule for carry trades.
   Always-long-the-spread when ``signal > entry_threshold`` (or
   always-short when configured). Exit when signal decays below
   ``exit_threshold`` or after ``max_hold_days``.

P&L per unit::

    long basis / long spread:  pnl = direction * (price_now - price_entry)
                                                            * contract_size

For carry strategies tracked via *net basis* (where lower = better for
long-basis), set ``invert=True`` so the engine flips the sign of the
price change.

The output is a pair of DataFrames:

- trades — one row per closed/open trade with entry/exit metadata + P&L
- nav    — per-date strategy NAV (cumulative P&L starting at 0)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd

REQUIRED_COLUMNS = ("date", "signal", "price")


@dataclass(frozen=True)
class BacktestRule:
    entry_threshold: float
    exit_threshold: float
    max_hold_days: int
    contract_size: float = 1.0      # RMB per 1.0 unit of price-spread move
    one_sided: bool = False         # True = directional carry (no shorts)
    long_when: str = "above"        # "above" or "below" — only used if one_sided
    invert_pnl: bool = False        # True flips sign (e.g. net basis convergence)


@dataclass
class Trade:
    strategy: str
    entry_date: str
    exit_date: str | None
    direction: int                  # +1 long, -1 short (the *spread*)
    entry_signal: float
    exit_signal: float | None
    entry_price: float
    exit_price: float | None
    holding_days: int
    pnl_per_unit: float | None      # in RMB (already × contract_size)
    exit_reason: str | None         # "target" / "max_hold" / "open"


@dataclass
class _Position:
    direction: int
    entry_date: str
    entry_signal: float
    entry_price: float


def _validate(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    if not df["date"].is_monotonic_increasing:
        raise ValueError("DataFrame must be sorted by date ascending")


def _direction_mean_reversion(signal: float) -> int:
    """Mean-revert: bet on reversion toward 0 — long the spread when
    signal is negative, short the spread when signal is positive."""
    return -1 if signal > 0 else 1


def _direction_one_sided(signal: float, rule: BacktestRule) -> int:
    if rule.long_when == "above":
        return 1 if signal > 0 else 0
    return -1 if signal < 0 else 0


def _apply_pnl_sign(direction: int, price_now: float, price_entry: float,
                    rule: BacktestRule) -> float:
    raw = (price_now - price_entry) * direction
    if rule.invert_pnl:
        raw = -raw
    return raw * rule.contract_size


def _close(pos: _Position, row: pd.Series, strategy: str,
           rule: BacktestRule, reason: str) -> Trade:
    pnl = _apply_pnl_sign(pos.direction, float(row["price"]),
                          pos.entry_price, rule)
    holding = (
        pd.to_datetime(row["date"]) - pd.to_datetime(pos.entry_date)
    ).days
    return Trade(
        strategy=strategy,
        entry_date=pos.entry_date,
        exit_date=str(row["date"]),
        direction=pos.direction,
        entry_signal=pos.entry_signal,
        exit_signal=float(row["signal"]),
        entry_price=pos.entry_price,
        exit_price=float(row["price"]),
        holding_days=holding,
        pnl_per_unit=pnl,
        exit_reason=reason,
    )


def _run(
    df: pd.DataFrame,
    *,
    strategy: str,
    rule: BacktestRule,
    pick_direction,
) -> tuple[list[Trade], list[dict]]:
    _validate(df)
    trades: list[Trade] = []
    nav_rows: list[dict] = []
    pos: _Position | None = None
    cumulative = 0.0

    for _, row in df.iterrows():
        date = str(row["date"])
        signal = float(row["signal"])
        price = float(row["price"])
        daily_pnl = 0.0

        if pos is None:
            direction = pick_direction(signal, rule) if rule.one_sided \
                else _direction_mean_reversion(signal)
            should_enter = (
                direction != 0 and abs(signal) >= rule.entry_threshold
                if not rule.one_sided
                else direction != 0 and (
                    (rule.long_when == "above" and signal >= rule.entry_threshold)
                    or (rule.long_when == "below" and signal <= -rule.entry_threshold)
                )
            )
            if should_enter:
                pos = _Position(
                    direction=direction,
                    entry_date=date,
                    entry_signal=signal,
                    entry_price=price,
                )
        else:
            # MTM the day's P&L vs prior price (reuse entry as anchor for
            # cumulative attribution)
            prev_close = nav_rows[-1]["price_for_open_pos"] if (
                nav_rows and nav_rows[-1].get("price_for_open_pos") is not None
            ) else pos.entry_price
            daily_pnl = _apply_pnl_sign(pos.direction, price, prev_close, rule)

            holding = (pd.to_datetime(date) - pd.to_datetime(pos.entry_date)).days
            should_exit = False
            reason = None
            if rule.one_sided:
                if rule.long_when == "above" and signal <= rule.exit_threshold:
                    should_exit, reason = True, "target"
                elif rule.long_when == "below" and signal >= -rule.exit_threshold:
                    should_exit, reason = True, "target"
            elif abs(signal) <= rule.exit_threshold:
                should_exit, reason = True, "target"
            if not should_exit and holding >= rule.max_hold_days:
                should_exit, reason = True, "max_hold"

            if should_exit:
                trades.append(_close(pos, row, strategy, rule, reason))
                pos = None

        cumulative += daily_pnl
        nav_rows.append({
            "date": date,
            "strategy": strategy,
            "signal": signal,
            "price": price,
            "in_position": pos is not None or daily_pnl != 0.0,
            "daily_pnl": daily_pnl,
            "cum_pnl": cumulative,
            "price_for_open_pos": price if pos is not None else None,
        })

    # Open trade at the end of the window — record as still-open
    if pos is not None:
        last = df.iloc[-1]
        trades.append(Trade(
            strategy=strategy,
            entry_date=pos.entry_date,
            exit_date=None,
            direction=pos.direction,
            entry_signal=pos.entry_signal,
            exit_signal=float(last["signal"]),
            entry_price=pos.entry_price,
            exit_price=float(last["price"]),
            holding_days=(pd.to_datetime(last["date"])
                          - pd.to_datetime(pos.entry_date)).days,
            pnl_per_unit=_apply_pnl_sign(pos.direction, float(last["price"]),
                                         pos.entry_price, rule),
            exit_reason="open",
        ))

    return trades, nav_rows


def run_mean_reversion(
    df: pd.DataFrame, *, strategy: str, rule: BacktestRule,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if rule.one_sided:
        raise ValueError("rule.one_sided=True; use run_directional_carry")
    trades, navs = _run(df, strategy=strategy, rule=rule,
                        pick_direction=_direction_mean_reversion)
    return _trades_df(trades), _nav_df(navs)


def run_directional_carry(
    df: pd.DataFrame, *, strategy: str, rule: BacktestRule,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not rule.one_sided:
        raise ValueError("rule.one_sided=False; use run_mean_reversion")
    trades, navs = _run(df, strategy=strategy, rule=rule,
                        pick_direction=_direction_one_sided)
    return _trades_df(trades), _nav_df(navs)


def _trades_df(trades: Iterable[Trade]) -> pd.DataFrame:
    rows = [t.__dict__ for t in trades]
    if not rows:
        return pd.DataFrame(columns=[
            "strategy", "entry_date", "exit_date", "direction",
            "entry_signal", "exit_signal", "entry_price", "exit_price",
            "holding_days", "pnl_per_unit", "exit_reason",
        ])
    return pd.DataFrame(rows)


def _nav_df(navs: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(navs)
    if "price_for_open_pos" in df.columns:
        df = df.drop(columns=["price_for_open_pos"])
    return df
