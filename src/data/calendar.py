"""Trading calendar — wraps AKShare's ``tool_trade_date_hist_sina``.

Cached locally as parquet so we only hit the network once per day.
"""

from __future__ import annotations

import datetime as dt
from functools import lru_cache
from pathlib import Path

import pandas as pd
from loguru import logger

from .storage import DATA_ROOT
from .utils import retry

CALENDAR_CACHE = DATA_ROOT / "parquet" / "calendar.parquet"
CACHE_TTL_HOURS = 12


@retry(max_attempts=3, initial_wait=2.0)
def _fetch_calendar() -> pd.DataFrame:
    """Pull A-share / interbank trading calendar from AKShare."""
    import akshare as ak

    df = ak.tool_trade_date_hist_sina()
    df = df.rename(columns={"trade_date": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.sort_values("date").reset_index(drop=True)


def _cache_is_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age = dt.datetime.now() - dt.datetime.fromtimestamp(path.stat().st_mtime)
    return age < dt.timedelta(hours=CACHE_TTL_HOURS)


def load_calendar(force_refresh: bool = False) -> pd.DataFrame:
    """Return the trading calendar, refreshing the local cache when stale."""
    if not force_refresh and _cache_is_fresh(CALENDAR_CACHE):
        return pd.read_parquet(CALENDAR_CACHE)

    logger.info("Refreshing trading calendar from AKShare")
    df = _fetch_calendar()
    CALENDAR_CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(CALENDAR_CACHE, index=False)
    return df


@lru_cache(maxsize=1)
def _trading_dates_set() -> frozenset[dt.date]:
    return frozenset(load_calendar()["date"].tolist())


def _to_date(d: str | dt.date | dt.datetime) -> dt.date:
    if isinstance(d, dt.datetime):
        return d.date()
    if isinstance(d, dt.date):
        return d
    return dt.datetime.strptime(d, "%Y-%m-%d").date()


def is_trading_day(d: str | dt.date) -> bool:
    return _to_date(d) in _trading_dates_set()


def previous_trading_day(d: str | dt.date) -> dt.date:
    """Latest trading day strictly before ``d``."""
    target = _to_date(d)
    cal = load_calendar()
    earlier = cal[cal["date"] < target]
    if earlier.empty:
        raise ValueError(f"No trading day before {target}")
    return earlier["date"].iloc[-1]


def next_trading_day(d: str | dt.date) -> dt.date:
    """Earliest trading day strictly after ``d``."""
    target = _to_date(d)
    cal = load_calendar()
    later = cal[cal["date"] > target]
    if later.empty:
        raise ValueError(f"No trading day after {target}")
    return later["date"].iloc[0]


def trading_days_between(start: str | dt.date, end: str | dt.date) -> list[dt.date]:
    """Inclusive list of trading days in [start, end]."""
    s, e = _to_date(start), _to_date(end)
    cal = load_calendar()
    mask = (cal["date"] >= s) & (cal["date"] <= e)
    return cal.loc[mask, "date"].tolist()


def latest_trading_day(today: str | dt.date | None = None) -> dt.date:
    """The most recent trading day on or before ``today`` (default: today)."""
    target = _to_date(today) if today else dt.date.today()
    if is_trading_day(target):
        return target
    return previous_trading_day(target)
