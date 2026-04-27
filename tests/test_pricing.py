"""Tests for the pricing engine: CF calculator and accrued interest."""

from __future__ import annotations

import datetime as dt

import pytest

from src.pricing.accrued import (
    compute_accrued,
    compute_accrued_simple,
    dirty_to_clean,
    previous_coupon_date,
)
from src.pricing.cf_calculator import (
    CFInputs,
    compute_cf,
    compute_cf_simple,
    months_30_360,
    next_coupon_date,
    parse_contract_id,
)


# ---- Date helpers -------------------------------------------------------


def test_parse_contract_id_t():
    product, delivery = parse_contract_id("T2606")
    assert product == "T"
    assert delivery == dt.date(2026, 6, 1)


def test_parse_contract_id_ts():
    product, delivery = parse_contract_id("TS2509")
    assert product == "TS"
    assert delivery == dt.date(2025, 9, 1)


def test_parse_contract_id_tl():
    assert parse_contract_id("TL2412") == ("TL", dt.date(2024, 12, 1))


def test_parse_contract_id_invalid():
    with pytest.raises(ValueError):
        parse_contract_id("XXX2606")


def test_next_coupon_basic():
    # Maturity 2034-08-25 → coupon date Aug 25 each year
    nxt = next_coupon_date(dt.date(2034, 8, 25), dt.date(2026, 6, 1))
    assert nxt == dt.date(2026, 8, 25)


def test_next_coupon_after_anniversary():
    # If we're past this year's coupon, step to next year
    nxt = next_coupon_date(dt.date(2034, 8, 25), dt.date(2026, 9, 1))
    assert nxt == dt.date(2027, 8, 25)


def test_next_coupon_feb29_fallback():
    # Maturity Feb 29 (leap-year bond) — non-leap year fallback to Feb 28
    nxt = next_coupon_date(dt.date(2032, 2, 29), dt.date(2026, 3, 1))
    assert nxt == dt.date(2027, 2, 28)


def test_months_30_360():
    assert months_30_360(dt.date(2026, 6, 1), dt.date(2026, 8, 25)) == \
        pytest.approx(2 + 24 / 30)
    assert months_30_360(dt.date(2026, 6, 1), dt.date(2027, 5, 25)) == \
        pytest.approx(11 + 24 / 30)


def test_months_30_360_negative_raises():
    with pytest.raises(ValueError):
        months_30_360(dt.date(2026, 6, 1), dt.date(2026, 5, 1))


# ---- CF formula spot-checks ---------------------------------------------
# Each tuple: (label, contract_id, coupon, maturity, official_cf, max_bp_diff)
SPOT_CHECKS = [
    ("T2606/240017",  "T2606",  0.0211, "2034-08-25", 0.9359, 0),
    ("T2606/230012",  "T2606",  0.0267, "2033-05-25", 0.9795, 0),
    ("T2606/230004",  "T2606",  0.0288, "2033-02-25", 0.9928, 1),
    ("T2606/250022",  "T2606",  0.0178, "2035-11-15", 0.9006, 2),
    ("TF2606/260008", "TF2606", 0.0150, "2031-04-15", 0.9334, 5),
    ("TS2606/260006", "TS2606", 0.0129, "2028-03-15", 0.9712, 6),
    ("TL2606/230009", "TL2606", 0.0319, "2053-04-15", 1.0348, 1),
]


@pytest.mark.parametrize("label,contract,coupon,maturity,official,max_bp",
                         SPOT_CHECKS, ids=[c[0] for c in SPOT_CHECKS])
def test_cf_within_tolerance(label, contract, coupon, maturity, official, max_bp):
    got = compute_cf_simple(coupon, maturity, contract)
    diff_bp = abs(got - official) * 10_000
    # 0.5 bp epsilon for floating-point comparison stability
    assert diff_bp <= max_bp + 0.5, (
        f"{label}: computed={got}, official={official}, diff={diff_bp:.2f}bp"
    )


def test_cf_breakdown_returns_intermediates():
    out = compute_cf(CFInputs(
        coupon_rate=0.0211,
        maturity=dt.date(2034, 8, 25),
        delivery_month_start=dt.date(2026, 6, 1),
    ))
    assert out.cf == pytest.approx(0.9359)
    assert out.next_coupon == dt.date(2026, 8, 25)
    assert out.x_months == pytest.approx(2 + 24 / 30)
    assert out.n_periods == 8


def test_cf_maturity_must_be_after_delivery():
    with pytest.raises(ValueError):
        compute_cf(CFInputs(
            coupon_rate=0.02,
            maturity=dt.date(2026, 5, 1),
            delivery_month_start=dt.date(2026, 6, 1),
        ))


# ---- Accrued interest ---------------------------------------------------


def test_previous_coupon_date():
    prev = previous_coupon_date(dt.date(2034, 8, 25), dt.date(2026, 6, 1))
    assert prev == dt.date(2025, 8, 25)


def test_previous_coupon_on_anniversary():
    # If valuation == coupon date, returned
    prev = previous_coupon_date(dt.date(2034, 8, 25), dt.date(2026, 8, 25))
    assert prev == dt.date(2026, 8, 25)


def test_accrued_zero_on_coupon_date():
    out = compute_accrued(0.025, "2034-08-25", "2025-08-25")
    assert out.accrued == pytest.approx(0.0)
    assert out.days_accrued == 0


def test_accrued_full_year_just_before_coupon():
    out = compute_accrued(0.025, "2034-08-25", "2026-08-24")
    # 1 day before next coupon → ~ full year accrued, slightly under coupon
    assert out.days_accrued == 364
    assert out.period_days == 365
    assert out.accrued == pytest.approx(0.025 * 100 * 364 / 365)


def test_accrued_half_year():
    # Roughly half a year of accrual
    out = compute_accrued(0.02, "2030-06-01", "2026-12-01")
    # period 2026-06-01 .. 2027-06-01 = 365 days
    # days from 2026-06-01 to 2026-12-01 = 183 days
    assert out.last_coupon == dt.date(2026, 6, 1)
    assert out.next_coupon == dt.date(2027, 6, 1)
    assert out.days_accrued == 183
    assert out.accrued == pytest.approx(0.02 * 100 * 183 / 365)


def test_accrued_act_365_alternate():
    out = compute_accrued(0.03, "2030-06-01", "2026-12-01", day_count="ACT/365")
    assert out.day_count == "ACT/365"
    assert out.accrued == pytest.approx(0.03 * 100 * out.days_accrued / 365)


def test_accrued_unsupported_day_count():
    with pytest.raises(ValueError):
        compute_accrued(0.02, "2030-06-01", "2026-12-01", day_count="30/360")


def test_accrued_after_maturity_raises():
    with pytest.raises(ValueError):
        compute_accrued(0.02, "2026-01-01", "2026-06-01")


def test_dirty_clean_roundtrip():
    accrued = 1.234
    clean = 99.500
    dirty = 100.734
    assert dirty_to_clean(dirty, accrued) == pytest.approx(clean)


def test_compute_accrued_simple_returns_number():
    val = compute_accrued_simple(0.02, "2030-06-01", "2026-12-01")
    assert isinstance(val, float)
    assert 0 < val < 2.0  # roughly half of annual coupon × 100
