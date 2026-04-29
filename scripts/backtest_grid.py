"""Parameter-sweep backtester for any strategy in the registry.

For each (entry_param × exit_param × max_hold_days) combination we run
the strategy through the existing engine and collect summary metrics
(``Sharpe``, ``hit_rate``, ``total_pnl``, ``max_drawdown``,
``avg_holding_days``). Results land in:

- ``parquet/backtest_grid/<grid_id>.parquet`` — one row per cell
- SQLite ``backtest_grid`` — same rows, indexed for ad-hoc query

The CLI accepts comma-separated value lists per axis or uses the
strategy's "sensible default" grid below.

Mean-reversion strategies (``calendar_mr_*``, ``curve_mr_*``) take
``entry_z`` / ``exit_z`` (z-score thresholds). The ``basis_long_carry``
strategy takes ``entry_bp`` / ``exit_bp`` (carry over funding, bp).
The runner picks the right kwarg names automatically based on the
strategy's signature.
"""

from __future__ import annotations

import argparse
import inspect
import itertools
import sys
import uuid
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from loguru import logger  # noqa: E402

from src.backtest.metrics import compute_metrics  # noqa: E402
from src.backtest.strategies import STRATEGY_REGISTRY  # noqa: E402
from src.data.storage import (  # noqa: E402
    init_schema,
    parquet_dir,
    sqlite_conn,
)
from src.data.utils import configure_logger  # noqa: E402


DEFAULT_Z_GRID = {
    "entry": (1.0, 1.5, 2.0, 2.5, 3.0),
    "exit": (0.0, 0.25, 0.5, 0.75, 1.0),
}
DEFAULT_BP_GRID = {
    "entry": (10.0, 20.0, 30.0, 40.0, 50.0),
    "exit": (-10.0, 0.0, 5.0, 10.0, 20.0),
}
DEFAULT_HOLD_GRID = (10, 15, 20, 30, 45)


def _runner_param_names(strategy: str) -> tuple[str, str]:
    """Return (entry_kw, exit_kw) for a strategy's runner.

    Inspects the runner's signature; for the curve closures the wrapper
    is kwargs-only, so we fall back on the well-known naming convention
    (``curve_mr_*`` always use z thresholds).
    """
    runner = STRATEGY_REGISTRY[strategy]
    params = inspect.signature(runner).parameters
    if "entry_z" in params:
        return "entry_z", "exit_z"
    if "entry_bp" in params:
        return "entry_bp", "exit_bp"
    if strategy.startswith("curve_mr_"):
        return "entry_z", "exit_z"
    raise RuntimeError(
        f"Cannot infer entry/exit kwargs for strategy {strategy!r}: "
        f"params={list(params)}"
    )


def _default_grids(strategy: str) -> tuple[tuple[float, ...], tuple[float, ...]]:
    entry_kw, _ = _runner_param_names(strategy)
    if entry_kw == "entry_bp":
        return DEFAULT_BP_GRID["entry"], DEFAULT_BP_GRID["exit"]
    return DEFAULT_Z_GRID["entry"], DEFAULT_Z_GRID["exit"]


def _parse_csv_floats(s: str | None) -> tuple[float, ...] | None:
    if not s:
        return None
    return tuple(float(x.strip()) for x in s.split(",") if x.strip())


def _parse_csv_ints(s: str | None) -> tuple[int, ...] | None:
    if not s:
        return None
    return tuple(int(x.strip()) for x in s.split(",") if x.strip())


def run_grid(
    *,
    strategy: str,
    entry_grid: tuple[float, ...],
    exit_grid: tuple[float, ...],
    hold_grid: tuple[int, ...],
) -> pd.DataFrame:
    """Execute the cartesian product and return a DataFrame of results.
    Cells where ``exit >= entry`` (no edge) are skipped.
    """
    runner = STRATEGY_REGISTRY[strategy]
    entry_kw, exit_kw = _runner_param_names(strategy)
    rows: list[dict] = []

    total = sum(
        1 for e, x, h in itertools.product(entry_grid, exit_grid, hold_grid)
        if x < e
    )
    logger.info(
        f"[{strategy}] running {total} valid cells "
        f"(entry={entry_grid}, exit={exit_grid}, hold={hold_grid})"
    )

    for entry, exit_, hold in itertools.product(entry_grid, exit_grid, hold_grid):
        if exit_ >= entry:
            continue  # exit must be strictly tighter than entry
        kwargs = {entry_kw: entry, exit_kw: exit_, "max_hold_days": hold}
        try:
            trades, nav, _ = runner(**kwargs)
            metrics = compute_metrics(trades, nav)
            rows.append({
                "strategy": strategy,
                "entry_param": float(entry),
                "exit_param": float(exit_),
                "max_hold_days": int(hold),
                **{k: getattr(metrics, k) for k in (
                    "n_trades", "hit_rate", "total_pnl", "sharpe_annualised",
                    "max_drawdown", "avg_holding_days",
                )},
            })
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"[{strategy}] cell entry={entry} exit={exit_} hold={hold} "
                f"failed: {exc}"
            )
    df = pd.DataFrame(rows).rename(
        columns={"sharpe_annualised": "sharpe"}
    )
    return df


def _persist(grid_id: str, strategy: str, df: pd.DataFrame) -> Path:
    if df.empty:
        raise RuntimeError("Grid produced 0 rows")

    out_dir = parquet_dir("backtest_grid")
    path = out_dir / f"{grid_id}.parquet"
    df_to_write = df.copy()
    df_to_write["grid_id"] = grid_id
    df_to_write.to_parquet(path, index=False, engine="pyarrow",
                           compression="snappy")

    with sqlite_conn() as conn:
        for _, r in df.iterrows():
            conn.execute(
                """INSERT OR REPLACE INTO backtest_grid
                   (grid_id, strategy, entry_param, exit_param,
                    max_hold_days, n_trades, hit_rate, total_pnl,
                    sharpe, max_drawdown, avg_holding_days)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (grid_id, strategy,
                 float(r["entry_param"]), float(r["exit_param"]),
                 int(r["max_hold_days"]), int(r["n_trades"]),
                 float(r["hit_rate"]) if pd.notna(r["hit_rate"]) else None,
                 float(r["total_pnl"]) if pd.notna(r["total_pnl"]) else None,
                 float(r["sharpe"]) if pd.notna(r["sharpe"]) else None,
                 float(r["max_drawdown"]) if pd.notna(r["max_drawdown"]) else None,
                 float(r["avg_holding_days"]) if pd.notna(r["avg_holding_days"]) else None),
            )
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run backtest grid sweep")
    parser.add_argument("--strategy", action="append", required=True,
                        choices=sorted(STRATEGY_REGISTRY),
                        help="May be repeated to sweep multiple strategies")
    parser.add_argument("--entry", help="Comma-separated entry-grid override")
    parser.add_argument("--exit", help="Comma-separated exit-grid override")
    parser.add_argument("--hold", help="Comma-separated max_hold_days override")
    parser.add_argument("--grid-id", help="Override generated grid_id (for "
                        "single-strategy runs only)")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    hold_grid = _parse_csv_ints(args.hold) or DEFAULT_HOLD_GRID
    entry_override = _parse_csv_floats(args.entry)
    exit_override = _parse_csv_floats(args.exit)

    summary = []
    for strategy in args.strategy:
        if entry_override is not None and exit_override is not None:
            entry_grid, exit_grid = entry_override, exit_override
        else:
            entry_grid, exit_grid = _default_grids(strategy)
            if entry_override is not None:
                entry_grid = entry_override
            if exit_override is not None:
                exit_grid = exit_override

        grid_id = (
            args.grid_id if (args.grid_id and len(args.strategy) == 1)
            else f"{strategy}_grid_{uuid.uuid4().hex[:8]}"
        )
        df = run_grid(
            strategy=strategy,
            entry_grid=entry_grid,
            exit_grid=exit_grid,
            hold_grid=hold_grid,
        )
        if df.empty:
            logger.warning(f"[{strategy}] grid empty; skipping persist")
            continue
        path = _persist(grid_id, strategy, df)

        best = df.sort_values("sharpe", ascending=False).iloc[0]
        logger.success(
            f"[{strategy}] cells={len(df)}  "
            f"best Sharpe={best['sharpe']:.2f} at entry={best['entry_param']}, "
            f"exit={best['exit_param']}, hold={int(best['max_hold_days'])}d  "
            f"-> {path.name}"
        )
        summary.append({
            "strategy": strategy,
            "grid_id": grid_id,
            "n_cells": len(df),
            "best_sharpe": float(best["sharpe"]),
            "best_entry": float(best["entry_param"]),
            "best_exit": float(best["exit_param"]),
            "best_hold": int(best["max_hold_days"]),
        })

    if summary:
        logger.info("Grid sweep summary:")
        for s in summary:
            logger.info(
                f"  {s['strategy']}: best Sharpe {s['best_sharpe']:+.2f} "
                f"@ entry={s['best_entry']}/exit={s['best_exit']}/"
                f"hold={s['best_hold']}d  ({s['n_cells']} cells, "
                f"grid_id={s['grid_id']})"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
