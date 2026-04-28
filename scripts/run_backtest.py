"""Run a strategy across the historical signal window and persist
results to ``parquet/backtest_runs/`` plus the SQLite ``backtest_runs``
table (params + summary metrics).

Usage::

    python3 scripts/run_backtest.py --strategy calendar_mr_T_near_far
    python3 scripts/run_backtest.py --strategy basis_long_carry_T

Override defaults with strategy-specific kwargs (e.g. ``--entry-z 1.5``)
or the more general ``--params key=value,key=value``.
"""

from __future__ import annotations

import argparse
import json
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


def _parse_kv(spec: str | None) -> dict:
    if not spec:
        return {}
    out: dict = {}
    for part in spec.split(","):
        if not part.strip():
            continue
        k, _, v = part.partition("=")
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        # try numeric coercion
        try:
            v_cast: float | int | str = int(v)
        except ValueError:
            try:
                v_cast = float(v)
            except ValueError:
                v_cast = v
        out[k] = v_cast
    return out


def _filter_window(df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    if df.empty:
        return df
    if start:
        df = df[df["date"] >= start]
    if end:
        df = df[df["date"] <= end]
    return df.reset_index(drop=True)


def _persist(
    *,
    run_id: str,
    strategy: str,
    trades: pd.DataFrame,
    nav: pd.DataFrame,
    params: dict,
    metrics,
) -> None:
    out_dir = parquet_dir("backtest_runs")
    trades_path = out_dir / f"{run_id}_trades.parquet"
    nav_path = out_dir / f"{run_id}_nav.parquet"

    if not trades.empty:
        trades.to_parquet(trades_path, index=False, engine="pyarrow",
                          compression="snappy")
    if not nav.empty:
        nav.to_parquet(nav_path, index=False, engine="pyarrow",
                       compression="snappy")

    start_date = nav["date"].iloc[0] if not nav.empty else ""
    end_date = nav["date"].iloc[-1] if not nav.empty else ""
    with sqlite_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO backtest_runs
               (run_id, strategy, start_date, end_date, params_json, metrics_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, strategy, start_date, end_date,
             json.dumps(params, sort_keys=True),
             json.dumps(asdict(metrics), sort_keys=True)),
        )

    logger.success(
        f"Persisted run_id={run_id}: trades={len(trades)} -> {trades_path.name},"
        f" nav={len(nav)} -> {nav_path.name}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a TBF backtest")
    parser.add_argument("--strategy", required=True,
                        choices=sorted(STRATEGY_REGISTRY))
    parser.add_argument("--start", help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive")
    parser.add_argument("--params", help="key=value,key=value override list")
    parser.add_argument("--run-id", help="Override generated run id")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    runner = STRATEGY_REGISTRY[args.strategy]
    overrides = _parse_kv(args.params)
    logger.info(f"Running {args.strategy} with overrides={overrides}")

    trades, nav, params = runner(**overrides)
    nav = _filter_window(nav, args.start, args.end)
    if not trades.empty:
        trades = trades[
            (trades["entry_date"].astype(str) >= (args.start or ""))
            & (trades["entry_date"].astype(str) <= (args.end or "9999-99-99"))
        ].reset_index(drop=True)

    metrics = compute_metrics(trades, nav)
    logger.info(f"Metrics: {metrics}")

    run_id = args.run_id or f"{args.strategy}_{uuid.uuid4().hex[:8]}"
    _persist(run_id=run_id, strategy=args.strategy,
             trades=trades, nav=nav, params=params, metrics=metrics)

    print(json.dumps({
        "run_id": run_id,
        "strategy": args.strategy,
        "metrics": asdict(metrics),
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
