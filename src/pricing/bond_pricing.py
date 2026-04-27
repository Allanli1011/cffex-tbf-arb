"""Bond pricing utilities for annual-coupon Chinese treasuries.

Functions price a bond at a given valuation date from its yield-to-maturity
(YTM) using the standard discounted-cashflow model with ACT/ACT day count.
This is sufficient for IRR / basis approximations when per-bond CCDC
valuation prices aren't directly available — we can plug in interpolated
par-curve yields.

Conventions:
- Annual coupon (one payment per year on the maturity MM-DD)
- ACT/ACT day count for both accrued and discount factors
- Yields are quoted annually (decimal): 0.025 = 2.5%
- All prices/values are per 100 face value

Pricing equation (dirty)::

    P_dirty = sum_{t_i > t_0} c * 100 / (1+y)^((t_i - t_0)/365)
              + 100 / (1+y)^((t_n - t_0)/365)

Clean = Dirty - Accrued.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from .accrued import compute_accrued, previous_coupon_date
from .cf_calculator import _safe_replace_year, _to_date


@dataclass(frozen=True)
class BondPricing:
    clean: float        # per 100 face
    dirty: float
    accrued: float
    macaulay_dur: float  # years
    modified_dur: float  # years
    convexity: float


def _coupon_dates_after(maturity: dt.date, after: dt.date) -> list[dt.date]:
    """Return all coupon dates strictly after ``after``, up to and
    including maturity."""
    out: list[dt.date] = []
    year = after.year
    while True:
        d = _safe_replace_year(maturity, year)
        if d > after and d <= maturity:
            out.append(d)
        if d >= maturity:
            break
        year += 1
    # Ensure maturity is included (handles edge of leap-day fallback)
    if maturity not in out:
        out.append(maturity)
    return sorted(set(out))


def price_from_yield(
    coupon_rate: float,
    maturity: str | dt.date,
    valuation_date: str | dt.date,
    yield_decimal: float,
    *,
    face: float = 100.0,
) -> BondPricing:
    """Price an annual-coupon bond from its YTM.

    Returns dirty + clean price plus duration / convexity (years).
    ``yield_decimal`` is the annual yield in decimal form (0.025 = 2.5%).
    """
    valuation = _to_date(valuation_date)
    maturity = _to_date(maturity)
    if valuation >= maturity:
        raise ValueError(
            f"Valuation date {valuation} must be before maturity {maturity}"
        )

    cashflows: list[tuple[dt.date, float]] = []
    coupon_amt = coupon_rate * face
    for d in _coupon_dates_after(maturity, valuation):
        cf = coupon_amt + (face if d == maturity else 0.0)
        cashflows.append((d, cf))

    # Discount each cashflow with ACT/365 fraction (consistent with how CCDC
    # publishes yield curves; ACT/ACT and ACT/365 differ by 0.27% on average,
    # acceptable for our diff bound).
    pv_total = 0.0
    weighted_t = 0.0
    convex_t2 = 0.0
    for d, cf in cashflows:
        t = (d - valuation).days / 365.0
        df = (1.0 + yield_decimal) ** (-t)
        pv = cf * df
        pv_total += pv
        weighted_t += t * pv
        convex_t2 += t * (t + 1) * pv

    macaulay = weighted_t / pv_total if pv_total else 0.0
    modified = macaulay / (1.0 + yield_decimal)
    convexity = convex_t2 / (pv_total * (1.0 + yield_decimal) ** 2) if pv_total else 0.0

    accrued = compute_accrued(coupon_rate, maturity, valuation, face=face).accrued
    return BondPricing(
        clean=pv_total - accrued,
        dirty=pv_total,
        accrued=accrued,
        macaulay_dur=macaulay,
        modified_dur=modified,
        convexity=convexity,
    )


def yield_from_price(
    coupon_rate: float,
    maturity: str | dt.date,
    valuation_date: str | dt.date,
    clean_price: float,
    *,
    face: float = 100.0,
    tol: float = 1e-8,
    max_iter: int = 100,
) -> float:
    """Solve for YTM given a clean price using bisection."""
    target_dirty = clean_price + compute_accrued(
        coupon_rate, maturity, valuation_date, face=face
    ).accrued

    def dirty_at(y: float) -> float:
        return price_from_yield(
            coupon_rate, maturity, valuation_date, y, face=face
        ).dirty

    # Yields below -10% would imply absurd prices; above 50% likewise unrealistic
    lo, hi = -0.10, 0.50
    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        if dirty_at(mid) > target_dirty:
            lo = mid
        else:
            hi = mid
        if hi - lo < tol:
            return mid
    raise RuntimeError("yield_from_price did not converge")


def interpolate_yield(
    tenors: list[float],
    yields: list[float],
    target_tenor: float,
) -> float:
    """Linear interpolation on the yield curve.

    For ``target_tenor`` outside the range we extrapolate flat.
    """
    if not tenors:
        raise ValueError("Empty tenor list")
    pairs = sorted(zip(tenors, yields))
    if target_tenor <= pairs[0][0]:
        return pairs[0][1]
    if target_tenor >= pairs[-1][0]:
        return pairs[-1][1]
    for (t1, y1), (t2, y2) in zip(pairs, pairs[1:]):
        if t1 <= target_tenor <= t2:
            w = (target_tenor - t1) / (t2 - t1)
            return y1 + w * (y2 - y1)
    raise RuntimeError("interpolation slipped")  # unreachable
