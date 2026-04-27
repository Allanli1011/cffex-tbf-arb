"""Tests for the pricing engine: CF calculator and accrued interest."""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

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


# ---- Bond pricing -------------------------------------------------------


def test_price_at_par_when_yield_equals_coupon():
    """A bond priced at YTM == coupon, on a coupon date, is approximately
    at par. Small (sub-bp per year) drift is expected because we use
    ACT/365 discounting while the schedule has leap years."""
    from src.pricing.bond_pricing import price_from_yield

    out = price_from_yield(
        coupon_rate=0.025,
        maturity="2030-08-25",
        valuation_date="2025-08-25",  # exactly on a past coupon date
        yield_decimal=0.025,
    )
    # 5Y horizon includes one leap year, expected drift < 10 bp
    assert out.dirty == pytest.approx(100.0, abs=0.05)
    assert out.accrued == pytest.approx(0.0)
    assert out.clean == pytest.approx(100.0, abs=0.05)


def test_price_inverse_yield_relationship():
    from src.pricing.bond_pricing import price_from_yield

    higher_yield = price_from_yield(0.025, "2030-08-25", "2026-04-27", 0.030)
    lower_yield = price_from_yield(0.025, "2030-08-25", "2026-04-27", 0.020)
    # Higher yield → lower price
    assert higher_yield.clean < lower_yield.clean


def test_yield_from_price_roundtrip():
    from src.pricing.bond_pricing import price_from_yield, yield_from_price

    target_y = 0.0212
    pr = price_from_yield(0.025, "2034-08-25", "2026-04-27", target_y)
    recovered = yield_from_price(0.025, "2034-08-25", "2026-04-27", pr.clean)
    assert recovered == pytest.approx(target_y, abs=1e-6)


def test_interpolate_yield():
    from src.pricing.bond_pricing import interpolate_yield

    tenors = [1.0, 3.0, 5.0, 10.0, 30.0]
    yields = [1.10, 1.30, 1.50, 1.80, 2.30]
    # Exact node
    assert interpolate_yield(tenors, yields, 5.0) == 1.50
    # Linear midpoint (5Y..10Y) → 1.65
    assert interpolate_yield(tenors, yields, 7.5) == pytest.approx(1.65)
    # Below first → flat extrapolation
    assert interpolate_yield(tenors, yields, 0.5) == 1.10
    # Above last → flat extrapolation
    assert interpolate_yield(tenors, yields, 50.0) == 2.30


def test_duration_positive_and_finite():
    from src.pricing.bond_pricing import price_from_yield

    out = price_from_yield(0.025, "2034-08-25", "2026-04-27", 0.020)
    assert 5.0 < out.modified_dur < 9.0    # ~7-8Y bond, plausible duration
    assert out.convexity > 0


# ---- IRR / basis --------------------------------------------------------


def test_basis_at_par_with_known_invoice():
    from src.pricing.irr import compute_basis

    # Construct a synthetic case where bond_clean = F * CF (invoice ≈ cost)
    out = compute_basis(
        valuation_date="2026-04-27",
        delivery_date="2026-06-12",
        bond_clean=100.0,
        coupon_rate=0.025,
        maturity="2034-08-25",
        futures=100.0,
        cf=1.0,
    )
    assert out.gross_basis == pytest.approx(0.0)
    # Invoice price = 100*1 + AI_T
    assert out.invoice_price == pytest.approx(100.0 + out.accrued_at_delivery)
    # No coupon during this short window (Apr-Jun, coupon date Aug)
    assert out.coupons_during == 0.0


def test_basis_irr_with_carry_period():
    """IRR should be negative when invoice barely exceeds cost (futures rich)."""
    from src.pricing.irr import compute_basis

    out = compute_basis(
        valuation_date="2026-04-27",
        delivery_date="2026-06-12",
        bond_clean=100.0,
        coupon_rate=0.0235,
        maturity="2034-08-25",
        futures=99.0,
        cf=1.0,
    )
    assert out.gross_basis == pytest.approx(1.0)  # 100 - 99*1
    # IRR should be negative (paying 100+AI_0, getting 99+AI_T < cost)
    assert out.irr_annualised < 0


def test_basis_invalid_dates_raise():
    from src.pricing.irr import compute_basis

    with pytest.raises(ValueError):
        compute_basis(
            valuation_date="2026-06-12",
            delivery_date="2026-04-27",   # before valuation
            bond_clean=100.0, coupon_rate=0.02,
            maturity="2034-08-25", futures=100.0, cf=1.0,
        )


def test_basis_coupon_during_window():
    """If a coupon falls inside the futures window, it adds to carry."""
    from src.pricing.irr import compute_basis

    # Coupon on 2026-08-25, valuation 2026-04-27, delivery 2026-09-12
    out = compute_basis(
        valuation_date="2026-04-27",
        delivery_date="2026-09-12",
        bond_clean=100.0, coupon_rate=0.025,
        maturity="2034-08-25", futures=100.0, cf=1.0,
    )
    assert out.coupons_during == pytest.approx(2.5)  # 0.025 * 100


def test_irr_minus_repo_bp():
    """Sanity: 2.5% IRR vs 1.5% repo → 100 bp."""
    from src.pricing.irr import irr_minus_repo_bp

    assert irr_minus_repo_bp(0.025, 1.5) == pytest.approx(100.0)
    assert irr_minus_repo_bp(0.010, 1.5) == pytest.approx(-50.0)


# ---- Realistic IRR end-to-end -------------------------------------------


def test_second_friday():
    """Helper used to compute CFFEX delivery dates."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    from compute_basis_signals import _second_friday

    # June 2026: 1=Mon, 5=Fri (1st), 12=Fri (2nd)
    assert _second_friday(2026, 6) == dt.date(2026, 6, 12)
    # September 2026: 1=Tue, 4=Fri (1st), 11=Fri (2nd)
    assert _second_friday(2026, 9) == dt.date(2026, 9, 11)
    # March 2025: 1=Sat, 7=Fri (1st), 14=Fri (2nd)
    assert _second_friday(2025, 3) == dt.date(2025, 3, 14)


# ---- Calendar spreads ---------------------------------------------------


def test_compute_spreads_three_contracts():
    """All three legs (near_mid / mid_far / near_far) should be emitted."""
    from src.pricing.spreads import compute_spreads_for_date
    import pandas as pd

    df = pd.DataFrame({
        "date": ["2026-04-24"] * 3,
        "contract_id": ["T2606", "T2609", "T2612"],
        "product": ["T", "T", "T"],
        "settle": [108.735, 108.650, 108.540],
    })
    spreads = compute_spreads_for_date(df)
    assert len(spreads) == 3
    legs = {s.leg for s in spreads}
    assert legs == {"near_mid", "mid_far", "near_far"}
    nm = next(s for s in spreads if s.leg == "near_mid")
    assert nm.spread == pytest.approx(108.650 - 108.735)


def test_compute_spreads_only_two_contracts():
    """With two contracts, only near_mid is emitted."""
    from src.pricing.spreads import compute_spreads_for_date
    import pandas as pd

    df = pd.DataFrame({
        "date": ["2026-04-24"] * 2,
        "contract_id": ["T2606", "T2609"],
        "product": ["T", "T"],
        "settle": [108.735, 108.650],
    })
    spreads = compute_spreads_for_date(df)
    assert len(spreads) == 1
    assert spreads[0].leg == "near_mid"


def test_compute_spreads_multi_product():
    """Spreads computed independently per product."""
    from src.pricing.spreads import compute_spreads_for_date
    import pandas as pd

    df = pd.DataFrame({
        "date": ["2026-04-24"] * 4,
        "contract_id": ["T2606", "T2609", "TF2606", "TF2609"],
        "product": ["T", "T", "TF", "TF"],
        "settle": [108.735, 108.650, 106.250, 106.105],
    })
    spreads = compute_spreads_for_date(df)
    products = {s.product for s in spreads}
    assert products == {"T", "TF"}


def test_days_diff_quarterly():
    from src.pricing.spreads import _delivery_month_diff_days
    # T2606 → T2609 = 3 months ≈ 90 days
    assert _delivery_month_diff_days("T2606", "T2609") == 90
    # Across year boundary
    assert _delivery_month_diff_days("T2612", "T2703") == 90


def test_rolling_zscore():
    from src.pricing.spreads import add_rolling_zscore
    import pandas as pd

    df = pd.DataFrame({
        "date": [f"2026-01-{i:02d}" for i in range(1, 41)],
        "product": ["T"] * 40,
        "leg": ["near_mid"] * 40,
        "near_contract": ["T2606"] * 40,
        "far_contract": ["T2609"] * 40,
        "near_settle": [100.0] * 40,
        "far_settle": [100.0] * 40,
        "spread": list(range(40)),       # monotonically increasing
        "days_diff": [90] * 40,
    })
    df = add_rolling_zscore(df, window=10, min_periods=5)
    # Final z should be positive (latest value above mean)
    assert df["z10"].iloc[-1] > 0
    # Within rolling window of 10 monotonic ints, latest is the max → percentile=1
    assert df["percentile10"].iloc[-1] == pytest.approx(1.0)
    # First few values are NaN (below min_periods)
    assert pd.isna(df["z10"].iloc[0])


def test_irr_realistic_t2606_240017():
    """Pull a real (futures, CF, bond) tuple and check IRR is plausible."""
    from src.pricing.bond_pricing import price_from_yield
    from src.pricing.irr import compute_basis

    # 2026-04-24 close: T2606 settle ~108.735, 240017 ytm ~ 1.76% (10Y curve)
    pr = price_from_yield(0.0211, "2034-08-25", "2026-04-24", 0.0176)
    out = compute_basis(
        valuation_date="2026-04-24",
        delivery_date="2026-06-12",
        bond_clean=pr.clean,
        coupon_rate=0.0211,
        maturity="2034-08-25",
        futures=108.735,
        cf=0.9359,
    )
    # IRR should be in a plausible range (-5% .. +5%) for sane inputs
    assert -0.05 < out.irr_annualised < 0.05
    assert out.gross_basis > -2.0  # bond clean shouldn't be wildly different
    assert out.gross_basis < 2.0
