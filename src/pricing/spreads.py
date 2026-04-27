"""Calendar spread (跨期价差) construction and statistics.

For each TBF product (TS / TF / T / TL) we look at the 3-4 simultaneously
listed quarterly contracts and emit, per trading day, three spread
series anchored to the active contracts sorted by delivery month::

    near        = M1
    mid         = M2  (a.k.a. next-near)
    far         = M3

    spread_near_mid = M2_settle - M1_settle
    spread_mid_far  = M3_settle - M2_settle
    spread_near_far = M3_settle - M1_settle

A larger far-minus-near means the curve is in stronger contango (carry).
Persistent dislocations from rolling mean (Z-score) are entry signals
for cross-quarter trades.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

PRODUCTS = ("TS", "TF", "T", "TL")
SPREAD_LEGS = ("near_mid", "mid_far", "near_far")


@dataclass(frozen=True)
class CalendarSpread:
    date: str
    product: str
    leg: str               # "near_mid" / "mid_far" / "near_far"
    near_contract: str
    far_contract: str
    near_settle: float
    far_settle: float
    spread: float          # far - near
    days_diff: int         # delivery-month gap in calendar days


def _delivery_yyyymm(contract_id: str) -> int:
    """``T2606`` -> 202606. Returns int for ordering."""
    digits = "".join(c for c in contract_id if c.isdigit())
    if len(digits) != 4:
        raise ValueError(f"unexpected contract_id {contract_id!r}")
    return 2000 * 100 + int(digits[:2]) * 100 + int(digits[2:])


def _delivery_month_diff_days(near: str, far: str) -> int:
    """Approximate calendar-day gap between two delivery months (M*30)."""
    near_yyyymm = _delivery_yyyymm(near)
    far_yyyymm = _delivery_yyyymm(far)
    months = (far_yyyymm // 100 - near_yyyymm // 100) * 12 + (
        far_yyyymm % 100 - near_yyyymm % 100
    )
    return int(months * 30)


def compute_spreads_for_date(futures_one_day: pd.DataFrame) -> list[CalendarSpread]:
    """Given a futures_daily slice for ONE date (all products), return
    all calendar spreads available.
    """
    if futures_one_day.empty:
        return []
    out: list[CalendarSpread] = []
    date = str(futures_one_day["date"].iloc[0])

    for product in PRODUCTS:
        sub = futures_one_day[futures_one_day["product"] == product]
        if len(sub) < 2:
            continue
        sub = sub.sort_values("contract_id").reset_index(drop=True)
        # Restrict to the first 3 by delivery month (CFFEX usually lists 4
        # but the most distant is illiquid in early life)
        sub = sub.head(3)
        contracts = sub["contract_id"].tolist()
        settles = sub["settle"].tolist()

        if len(contracts) >= 2:
            out.append(_make("near_mid", date, product, contracts[0],
                             contracts[1], settles[0], settles[1]))
        if len(contracts) >= 3:
            out.append(_make("mid_far", date, product, contracts[1],
                             contracts[2], settles[1], settles[2]))
            out.append(_make("near_far", date, product, contracts[0],
                             contracts[2], settles[0], settles[2]))
    return out


def _make(leg: str, date: str, product: str,
          near: str, far: str,
          near_settle: float, far_settle: float) -> CalendarSpread:
    return CalendarSpread(
        date=date, product=product, leg=leg,
        near_contract=near, far_contract=far,
        near_settle=near_settle, far_settle=far_settle,
        spread=far_settle - near_settle,
        days_diff=_delivery_month_diff_days(near, far),
    )


# ---- Rolling statistics --------------------------------------------------


def add_rolling_zscore(df: pd.DataFrame, window: int = 60,
                       min_periods: int = 30) -> pd.DataFrame:
    """For each (product, leg) series, add ``z<window>`` and
    ``percentile<window>`` (range 0-1) columns based on a backward rolling
    window. ``min_periods`` controls when stats begin to populate.

    The frame must already be sorted by date within each (product, leg)
    group.
    """
    if df.empty:
        return df
    df = df.sort_values(["product", "leg", "date"]).reset_index(drop=True)
    z_col = f"z{window}"
    p_col = f"percentile{window}"
    df[z_col] = None
    df[p_col] = None

    for (product, leg), idx in df.groupby(["product", "leg"]).groups.items():
        sub = df.loc[idx, "spread"]
        mean = sub.rolling(window, min_periods=min_periods).mean()
        std = sub.rolling(window, min_periods=min_periods).std()
        z = (sub - mean) / std
        # Percentile: rank position over the trailing window
        pct = sub.rolling(window, min_periods=min_periods).apply(
            lambda s: (s.rank(pct=True).iloc[-1]), raw=False
        )
        df.loc[idx, z_col] = z.astype("float64")
        df.loc[idx, p_col] = pct.astype("float64")
    return df


def to_dataframe(spreads: list[CalendarSpread]) -> pd.DataFrame:
    """Materialise a list of CalendarSpread into a DataFrame."""
    if not spreads:
        return pd.DataFrame(columns=[
            "date", "product", "leg", "near_contract", "far_contract",
            "near_settle", "far_settle", "spread", "days_diff",
        ])
    return pd.DataFrame([s.__dict__ for s in spreads])
