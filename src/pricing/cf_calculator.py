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


def next_coupon_date(maturity: dt.date, on_or_after: dt.date) -> dt.date:
    """For an annual coupon bond, the next coupon date on or after a given date.

    Coupons fall on the same MM-DD as ``maturity`` each year (Chinese
    treasury convention). If ``on_or_after`` already past this year's
    coupon, we step to next year. Day-of-month is preserved with
    Feb-29 falling back to Feb-28 in non-leap years.
    """
    year = on_or_after.year
    candidate = _safe_replace_year(maturity, year)
    if candidate < on_or_after:
        candidate = _safe_replace_year(maturity, year + 1)
    return candidate


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


@dataclass(frozen=True)
class CFBreakdown:
    cf: float                # rounded to 4 decimals
    cf_raw: float            # unrounded
    x_months: float
    n_periods: int
    next_coupon: dt.date


def compute_cf(inputs: CFInputs) -> CFBreakdown:
    """Compute the conversion factor for an annual-coupon bond."""
    r = inputs.notional_rate
    c = inputs.coupon_rate
    delivery = inputs.delivery_month_start
    maturity = inputs.maturity

    if maturity <= delivery:
        raise ValueError(
            f"Maturity {maturity} is not after delivery {delivery}"
        )

    nxt = next_coupon_date(maturity, delivery)
    x = months_30_360(delivery, nxt)
    n = max(maturity.year - nxt.year, 0)

    pow_x = (1.0 + r) ** (x / 12.0)
    pow_n = (1.0 + r) ** n

    bracket = c + c / r + (1.0 - c / r) / pow_n
    cf_raw = (1.0 / pow_x) * bracket - c * (1.0 - x / 12.0)
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
) -> float:
    """Convenience wrapper returning just the rounded CF."""
    _, delivery = parse_contract_id(contract_id)
    return compute_cf(CFInputs(
        coupon_rate=coupon_rate,
        maturity=_to_date(maturity),
        delivery_month_start=delivery,
    )).cf
