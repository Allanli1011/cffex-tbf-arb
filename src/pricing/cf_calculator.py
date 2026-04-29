"""Conversion factor calculator — implements CFFEX's official formula.

Reference: CFFEX 5/10/30 年期国债期货合约交割细则 附件 A.

The formula assumes:
- Annual coupon payment (f = 1) — matches Chinese government bonds
- Notional rate r = 3.0%
- Delivery date = first day of contract delivery month
- 30/360 month-fraction convention for the partial period x

Formula::

    CF = (1+r)^(-x/12) * [c + c/r + (1 - c/r) * (1+r)^(-n)]
         - c * (1 - x/12)

Where:
    r = 0.03
    c = bond annual coupon (decimal)
    x = months from delivery month start to next coupon date,
        expressed as ``whole_months + day_offset / 30``
    n = number of coupon payments after the *next* one
        (so ``n = 0`` if the next coupon coincides with maturity)

The result is rounded to 4 decimal places to match CFFEX's published
precision.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

NOTIONAL_RATE = 0.03


# ---- date helpers -------------------------------------------------------


def _to_date(d: str | dt.date) -> dt.date:
    if isinstance(d, dt.date):
        return d
    return dt.date.fromisoformat(d)


def parse_contract_id(contract_id: str) -> tuple[str, dt.date]:
    """Split ``T2606`` -> (``T``, 2026-06-01).

    For TS/TF/T/TL contracts the last 4 digits encode YYMM; we anchor at
    the first day of the delivery month, which is CFFEX's CF reference.
    """
    contract_id = contract_id.strip()
    for prefix in ("TS", "TF", "TL", "T"):
        if contract_id.startswith(prefix) and contract_id[len(prefix):].isdigit():
            yymm = contract_id[len(prefix):]
            if len(yymm) != 4:
                continue
            year = 2000 + int(yymm[:2])
            month = int(yymm[2:])
            if 1 <= month <= 12:
                return prefix, dt.date(year, month, 1)
    raise ValueError(f"Cannot parse contract_id {contract_id!r}")


def next_coupon_date(maturity: dt.date, on_or_after: dt.date, coupon_frequency: int = 1) -> dt.date:
    """For an annual or semi-annual coupon bond, the next coupon date on or after a given date.

    Coupons fall on the same MM-DD as ``maturity`` each year, and possibly 6 months apart
    if semi-annual. Day-of-month is preserved with Feb-29 falling back to Feb-28.
    """
    if coupon_frequency not in (1, 2):
        raise ValueError(f"Unsupported coupon_frequency {coupon_frequency}")
    
    year = on_or_after.year
    candidates = []
    candidates.append(_safe_replace_year(maturity, year - 1))
    candidates.append(_safe_replace_year(maturity, year))
    candidates.append(_safe_replace_year(maturity, year + 1))
    
    if coupon_frequency == 2:
        # Add the 6-month offset candidates
        def _add_6_months(d: dt.date) -> dt.date:
            m = d.month + 6
            y = d.year
            if m > 12:
                m -= 12
                y += 1
            try:
                return dt.date(y, m, d.day)
            except ValueError:
                # E.g., Aug 31 -> Feb 28/29
                if m == 2:
                    # check leap year
                    is_leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
                    return dt.date(y, m, 29 if is_leap else 28)
                elif d.day == 31 and m in (4, 6, 9, 11):
                    return dt.date(y, m, 30)
                raise
        
        semi_candidates = [_add_6_months(c) for c in candidates]
        candidates.extend(semi_candidates)
        
    candidates.sort()
    for candidate in candidates:
        if candidate >= on_or_after:
            return candidate
    return candidates[-1]


def _safe_replace_year(d: dt.date, new_year: int) -> dt.date:
    """``date.replace(year=...)`` that tolerates Feb 29."""
    try:
        return d.replace(year=new_year)
    except ValueError:
        return d.replace(year=new_year, day=28)


def months_30_360(start: dt.date, end: dt.date) -> float:
    """Months between two dates using 30/360 day count.

    Computed as ``year_diff*12 + month_diff + (day_end - day_start)/30``.
    Negative day offsets are allowed (will reduce the integer-month tally).
    """
    if end < start:
        raise ValueError(f"end {end} must be on or after start {start}")
    months = (end.year - start.year) * 12 + (end.month - start.month)
    day_offset = (end.day - start.day) / 30
    return months + day_offset


# ---- CF computation -----------------------------------------------------


@dataclass(frozen=True)
class CFInputs:
    coupon_rate: float       # decimal, e.g. 0.0211 for 2.11%
    maturity: dt.date
    delivery_month_start: dt.date
    notional_rate: float = NOTIONAL_RATE
    coupon_frequency: int = 1


@dataclass(frozen=True)
class CFBreakdown:
    cf: float                # rounded to 4 decimals
    cf_raw: float            # unrounded
    x_months: float
    n_periods: int
    next_coupon: dt.date


def compute_cf(inputs: CFInputs) -> CFBreakdown:
    """Compute the conversion factor for an annual or semi-annual coupon bond."""
    r = inputs.notional_rate
    c = inputs.coupon_rate
    f = inputs.coupon_frequency
    delivery = inputs.delivery_month_start
    maturity = inputs.maturity

    if maturity <= delivery:
        raise ValueError(
            f"Maturity {maturity} is not after delivery {delivery}"
        )

    nxt = next_coupon_date(maturity, delivery, f)
    x = months_30_360(delivery, nxt)
    
    # n is the number of remaining periods AFTER the next coupon
    # Calculated based on months between nxt and maturity, divided by (12/f)
    months_to_maturity = (maturity.year - nxt.year) * 12 + (maturity.month - nxt.month)
    # round to nearest integer period
    n = max(int(round(months_to_maturity / (12.0 / f))), 0)

    pow_x = (1.0 + r / f) ** (x / (12.0 / f))
    pow_n = (1.0 + r / f) ** n

    # c/f is coupon per period
    bracket = c / f + c / r + (1.0 - c / r) / pow_n
    # Note: 100 face value is implied as 1.0 in the bracket term (c/r + (1-c/r)/pow_n)
    # wait, the formula is: [ c/f + c/r*(1 - 1/(1+r/f)^n) + 1/(1+r/f)^n ]
    # bracket = c/f + c/r - (c/r)/pow_n + 1/pow_n = c/f + c/r + (1 - c/r) / pow_n
    # This is algebraically identical to [ c/f + c/r*(1 - 1/(1+r/f)^n) + 1/(1+r/f)^n ]
    cf_raw = (1.0 / pow_x) * bracket - (c / f) * (1.0 - x / (12.0 / f))
    return CFBreakdown(
        cf=round(cf_raw, 4),
        cf_raw=cf_raw,
        x_months=x,
        n_periods=n,
        next_coupon=nxt,
    )


def compute_cf_simple(
    coupon_rate: float,
    maturity: str | dt.date,
    contract_id: str,
    coupon_frequency: int = 1,
) -> float:
    """Convenience wrapper returning just the rounded CF."""
    _, delivery = parse_contract_id(contract_id)
    return compute_cf(CFInputs(
        coupon_rate=coupon_rate,
        maturity=_to_date(maturity),
        delivery_month_start=delivery,
        coupon_frequency=coupon_frequency,
    )).cf
