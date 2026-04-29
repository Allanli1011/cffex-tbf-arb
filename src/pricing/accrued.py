"""Accrued interest for Chinese government bonds.

Convention (matches 中债估值 / CCDC valuation):
- Annual coupon (one payment per year on the same MM-DD as maturity)
- Day count: ACT/ACT — accrued ratio = days_since_last_coupon /
  days_in_current_coupon_period

The ``day_count`` parameter also supports ``ACT/365`` for cross-checks
against alternative conventions.

Returns accrued interest in **per-100-face** units (so a 2.00% coupon
bond half-way through the year gives ~1.00).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from .cf_calculator import _safe_replace_year, next_coupon_date, _to_date

DEFAULT_DAY_COUNT = "ACT/ACT"
SUPPORTED_DAY_COUNTS = {"ACT/ACT", "ACT/365"}


@dataclass(frozen=True)
class AccruedBreakdown:
    accrued: float           # per 100 face
    last_coupon: dt.date
    next_coupon: dt.date
    days_accrued: int
    period_days: int
    day_count: str


def previous_coupon_date(maturity: dt.date, before_or_on: dt.date, coupon_frequency: int = 1) -> dt.date:
    """Most recent coupon date on or before ``before_or_on``.

    Coupons fall on the maturity MM-DD each year, or also 6 months apart if semi-annual.
    """
    if coupon_frequency not in (1, 2):
        raise ValueError(f"Unsupported coupon_frequency {coupon_frequency}")
        
    year = before_or_on.year
    candidates = []
    candidates.append(_safe_replace_year(maturity, year - 1))
    candidates.append(_safe_replace_year(maturity, year))
    candidates.append(_safe_replace_year(maturity, year + 1))
    
    if coupon_frequency == 2:
        def _add_6_months(d: dt.date) -> dt.date:
            m = d.month + 6
            y = d.year
            if m > 12:
                m -= 12
                y += 1
            try:
                return dt.date(y, m, d.day)
            except ValueError:
                if m == 2:
                    is_leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
                    return dt.date(y, m, 29 if is_leap else 28)
                elif d.day == 31 and m in (4, 6, 9, 11):
                    return dt.date(y, m, 30)
                raise
                
        semi_candidates = [_add_6_months(c) for c in candidates]
        candidates.extend(semi_candidates)
        
    candidates.sort(reverse=True)
    for candidate in candidates:
        if candidate <= before_or_on:
            return candidate
    return candidates[-1]


def compute_accrued(
    coupon_rate: float,
    maturity: str | dt.date,
    valuation_date: str | dt.date,
    *,
    day_count: str = DEFAULT_DAY_COUNT,
    face: float = 100.0,
    coupon_frequency: int = 1,
) -> AccruedBreakdown:
    """Compute accrued interest for a Chinese treasury.

    Parameters
    ----------
    coupon_rate:
        Annual coupon rate, decimal (e.g. ``0.0211`` for 2.11%).
    maturity:
        Bond maturity date.
    valuation_date:
        Date at which accrued is evaluated.
    day_count:
        ``ACT/ACT`` (default) or ``ACT/365``.
    face:
        Face value scale; accrued is returned per ``face`` units.
    coupon_frequency:
        1 for annual, 2 for semi-annual.
    """
    if day_count not in SUPPORTED_DAY_COUNTS:
        raise ValueError(
            f"Unsupported day_count {day_count!r}. "
            f"Supported: {sorted(SUPPORTED_DAY_COUNTS)}"
        )

    maturity = _to_date(maturity)
    valuation = _to_date(valuation_date)
    if valuation > maturity:
        raise ValueError(
            f"Valuation date {valuation} is after maturity {maturity}"
        )

    prev = previous_coupon_date(maturity, valuation, coupon_frequency)
    nxt = next_coupon_date(maturity, valuation, coupon_frequency)
    if nxt <= prev:  # safety: e.g. valuation on coupon date
        # if valuation is on coupon date, next_coupon_date returns the exact same date!
        # wait, next_coupon_date says "on or after". So it returns the same date.
        # But for accrued, we need the strictly *next* coupon date for the period.
        nxt = next_coupon_date(maturity, valuation + dt.timedelta(days=1), coupon_frequency)

    days_accrued = (valuation - prev).days
    period_days = (nxt - prev).days

    if day_count == "ACT/ACT":
        ratio = days_accrued / period_days if period_days > 0 else 0.0
    else:  # ACT/365
        ratio = days_accrued / 365

    # ACT/ACT standard says we divide the annual coupon by frequency
    # then multiply by (days_accrued / period_days). Wait, is that right?
    # Yes, for semi-annual, it's (c/2) * (days_accrued / period_days).
    # This is algebraically the same as c * (days_accrued / (frequency * period_days)).
    if day_count == "ACT/ACT":
        accrued = (coupon_rate / coupon_frequency) * face * ratio
    else:
        # for ACT/365, ratio is days/365, so accrued is just c * days / 365
        accrued = coupon_rate * face * ratio

    return AccruedBreakdown(
        accrued=accrued,
        last_coupon=prev,
        next_coupon=nxt,
        days_accrued=days_accrued,
        period_days=period_days,
        day_count=day_count,
    )


def compute_accrued_simple(
    coupon_rate: float,
    maturity: str | dt.date,
    valuation_date: str | dt.date,
    coupon_frequency: int = 1,
) -> float:
    """Convenience wrapper returning just the accrued figure (per 100 face)."""
    return compute_accrued(coupon_rate, maturity, valuation_date, coupon_frequency=coupon_frequency).accrued


def dirty_to_clean(dirty_price: float, accrued: float) -> float:
    """Clean = dirty - accrued. All quantities per same face scale."""
    return dirty_price - accrued


def clean_to_dirty(clean_price: float, accrued: float) -> float:
    """Dirty = clean + accrued."""
    return clean_price + accrued
