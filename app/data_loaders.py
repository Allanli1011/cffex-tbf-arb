"""Cached loaders for the Streamlit panel.

Each loader reads parquet / sqlite from the project's data layout and
returns a tidy ``pandas.DataFrame``. Streamlit's ``cache_data`` is used
so navigating between tabs doesn't re-read parquet from disk every time.

Cache TTL is short (5 minutes) — long enough to keep the panel snappy,
short enough that a fresh ETL run shows up in the next tab switch.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from src.data.storage import PARQUET_DATASETS, sqlite_conn

CACHE_TTL_SECONDS = 300


def _concat(dataset: str, *, start: str | None = None,
            end: str | None = None) -> pd.DataFrame:
    files = sorted(PARQUET_DATASETS[dataset].glob("*.parquet"))
    if start:
        files = [f for f in files if f.stem >= start]
    if end:
        files = [f for f in files if f.stem <= end]
    if not files:
        return pd.DataFrame()
    parts = [pd.read_parquet(f) for f in files]
    df = pd.concat(parts, ignore_index=True)
    if "date" in df.columns:
        df = df.sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_basis_signals(start: str | None = None,
                       end: str | None = None) -> pd.DataFrame:
    return _concat("basis_signals", start=start, end=end)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_calendar_spreads(start: str | None = None,
                          end: str | None = None) -> pd.DataFrame:
    return _concat("calendar_spreads", start=start, end=end)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_curve_signals(start: str | None = None,
                       end: str | None = None) -> pd.DataFrame:
    return _concat("curve_signals", start=start, end=end)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_ctd_switch(start: str | None = None,
                    end: str | None = None) -> pd.DataFrame:
    return _concat("ctd_switch", start=start, end=end)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_futures_daily(start: str | None = None,
                       end: str | None = None) -> pd.DataFrame:
    return _concat("futures_daily", start=start, end=end)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_repo_rate(start: str | None = None,
                   end: str | None = None) -> pd.DataFrame:
    return _concat("repo_rate", start=start, end=end)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_backtest_runs() -> pd.DataFrame:
    """One row per run from the SQLite ``backtest_runs`` table."""
    with sqlite_conn() as conn:
        df = pd.read_sql_query(
            """SELECT run_id, strategy, start_date, end_date,
                      params_json, metrics_json, created_at
               FROM backtest_runs
               ORDER BY created_at DESC""",
            conn,
        )
    if df.empty:
        return df
    # Decode JSON columns into a dict (kept as a column for downstream use)
    df["params"] = df["params_json"].apply(_safe_json)
    df["metrics"] = df["metrics_json"].apply(_safe_json)
    return df


def _safe_json(s: str | None) -> dict:
    if not s:
        return {}
    try:
        return json.loads(s)
    except (ValueError, TypeError):
        return {}


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_backtest_run_artifacts(run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(trades_df, nav_df)`` for the given run, empty if missing."""
    base = PARQUET_DATASETS["backtest_runs"]
    trades_path = base / f"{run_id}_trades.parquet"
    nav_path = base / f"{run_id}_nav.parquet"
    trades = pd.read_parquet(trades_path) if trades_path.exists() else pd.DataFrame()
    nav = pd.read_parquet(nav_path) if nav_path.exists() else pd.DataFrame()
    return trades, nav


def latest_date(*frames: pd.DataFrame) -> str | None:
    """Across multiple frames with a ``date`` column, return the maximum."""
    dates = []
    for df in frames:
        if df is not None and not df.empty and "date" in df.columns:
            dates.append(str(df["date"].max()))
    return max(dates) if dates else None
