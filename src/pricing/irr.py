"""IRR / basis / net basis calculator.

Given the standard set of inputs for a (futures, deliverable bond) pair:

    F      = futures price (settle, per 100 face)
    CF     = conversion factor
    P_clean = bond clean price (per 100 face)
    AI_0    = accrued interest at valuation date
    AI_T    = accrued interest at delivery date
    coupons_during = total coupons received between valuation and delivery
    n_days = days from valuation to delivery
    repo_rate (optional, for net basis only)

The relationships are::

    InvoicePrice  = F * CF + AI_T
    Cost          = P_clean + AI_0  - coupons_during
    GrossBasis    = P_clean - F * CF
    Carry         = AI_T - AI_0 + coupons_during            # absolute carry per 100 face
    NetBasis      = GrossBasis - Carry
    IRR           = (InvoicePrice + coupons_during - Cost') / Cost' * 365 / n_days
                  where Cost' = P_clean + AI_0 (gross investment)

Note: the IRR formula treats coupons received during the holding period
as positive cash inflows added to the invoice price (standard CME-style).
For the typical Chinese-treasury case (annual coupon), the holding
window for futures contracts (~3-12 months) usually contains zero or
one coupon.

The :class:`BasisQuote` dataclass returns all components so callers can
inspect intermediates without re-computing.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from .accrued import compute_accrued
from .cf_calculator import _to_date


@dataclass(frozen=True)
class BasisQuote:
    valuation_date: dt.date
    delivery_date: dt.date
    bond_clean: float
    futures: float
    cf: float
    accrued_now: float
    accrued_at_delivery: float
    coupons_during: float
    invoice_price: float
    gross_basis: float
    carry: float
    net_basis: float
    irr_annualised: float           # decimal, e.g. 0.025 = 2.5%
    n_days: int


def coupons_received_in_window(
    coupon_rate: float,
    maturity: dt.date,
    start: dt.date,
    end: dt.date,
    *,
    face: float = 100.0,
) -> tuple[float, list[dt.date]]:
    """Sum of coupons strictly between ``start`` (exclusive) and ``end``
    (inclusive). For an annual-coupon bond this is at most one coupon
    over a typical futures window.
    """
    if end < start:
        raise ValueError("end must be on or after start")
    coupons: list[dt.date] = []
    year = start.year
    coupon_amt = coupon_rate * face
    while True:
        try:
            d = maturity.replace(year=year)
        except ValueError:  # Feb 29 -> Feb 28 fallback
            d = maturity.replace(year=year, day=28)
        if d > end:
            break
        if d > start:
            coupons.append(d)
        year += 1
        if year > end.year + 1:
            break
    return coupon_amt * len(coupons), coupons


def compute_basis(
    *,
    valuation_date: str | dt.date,
    delivery_date: str | dt.date,
    bond_clean: float,
    coupon_rate: float,
    maturity: str | dt.date,
    futures: float,
    cf: float,
    face: float = 100.0,
) -> BasisQuote:
    """Compute basis / carry / IRR for a (futures, bond) pair.

    All prices are per ``face`` units. ``coupon_rate`` is decimal
    (0.0211 for 2.11%).
    """
    valuation = _to_date(valuation_date)
    delivery = _to_date(delivery_date)
    maturity_d = _to_date(maturity)
    if delivery <= valuation:
        raise ValueError(f"Delivery {delivery} must be after valuation {valuation}")
    if delivery > maturity_d:
        raise ValueError(
            f"Delivery {delivery} is after bond maturity {maturity_d}"
        )

    accrued_now = compute_accrued(
        coupon_rate, maturity_d, valuation, face=face
    ).accrued
    accrued_at_delivery = compute_accrued(
        coupon_rate, maturity_d, delivery, face=face
    ).accrued
    coupons_during, _ = coupons_received_in_window(
        coupon_rate, maturity_d, valuation, delivery, face=face
    )

    invoice = futures * cf + accrued_at_delivery
    gross_basis = bond_clean - futures * cf
    carry = (accrued_at_delivery - accrued_now) + coupons_during
    net_basis = gross_basis - carry

    cost = bond_clean + accrued_now
    if cost <= 0:
        raise ValueError(f"Bond total price {cost} must be positive")
    n_days = (delivery - valuation).days
    irr = ((invoice + coupons_during) / cost - 1.0) * 365.0 / n_days

    return BasisQuote(
        valuation_date=valuation,
        delivery_date=delivery,
        bond_clean=bond_clean,
        futures=futures,
        cf=cf,
        accrued_now=accrued_now,
        accrued_at_delivery=accrued_at_delivery,
        coupons_during=coupons_during,
        invoice_price=invoice,
        gross_basis=gross_basis,
        carry=carry,
        net_basis=net_basis,
        irr_annualised=irr,
        n_days=n_days,
    )


def irr_minus_repo_bp(irr_decimal: float, repo_pct: float) -> float:
    """IRR (decimal) minus repo (in percent) in basis points.

    Our stored rates (FDR007, GC007, Shibor) are in percent units, while
    IRR is computed as a decimal — convert and difference.

    Positive ⇒ futures cheap relative to financing (positive-basis trade).
    Negative ⇒ futures rich relative to financing (reverse trade if feasible).
    """
    return (irr_decimal - repo_pct / 100.0) * 10_000
