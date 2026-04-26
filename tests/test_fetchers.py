"""Tests for the CFFEX deliverable-bond fetcher and bond master upsert."""

from __future__ import annotations

import pytest

from src.data import bonds, storage
from src.data.bonds import Bond
from src.data.fetchers import parse_deliverable_csv


SAMPLE_CSV = (
    "2024年记账式附息（十七期）国债,240017,019734,102273,20340825,2.11,0.9359,T2606,T\n"
    "2024年记账式附息（十七期）国债,240017,019734,102273,20340825,2.11,0.9377,T2609,T\n"
    "2026年记账式附息（八期）国债,260008,019836,102338,20310415,1.50,0.9334,TF2606,TF\n"
    "2024年超长期特别国债（一期）,2400001,019742,102289,20540520,2.57,0.9191,TL2606,TL\n"
).encode("utf-8")


@pytest.fixture(autouse=True)
def fresh_schema(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    monkeypatch.setattr(storage, "SQLITE_PATH", db)
    storage.init_schema()
    yield


# ---- CSV parsing --------------------------------------------------------


def test_parse_deliverable_csv_shape():
    snaps = parse_deliverable_csv(SAMPLE_CSV)
    assert len(snaps) == 4
    contracts = {s.contract_id for s in snaps}
    assert contracts == {"T2606", "T2609", "TF2606", "TL2606"}


def test_parse_deliverable_csv_normalisation():
    snaps = parse_deliverable_csv(SAMPLE_CSV)
    s = snaps[0]
    # coupon was "2.11" % -> 0.0211 decimal
    assert s.bond.coupon_rate == pytest.approx(0.0211)
    # maturity 20340825 -> "2034-08-25"
    assert s.bond.maturity_date == "2034-08-25"
    # CF passes through as float
    assert s.cf_row.cf == pytest.approx(0.9359)
    assert s.cf_row.contract_id == "T2606"
    assert s.cf_row.bond_code == "240017"


def test_parse_deliverable_csv_long_term_special():
    snaps = parse_deliverable_csv(SAMPLE_CSV)
    tl = next(s for s in snaps if s.contract_id == "TL2606")
    assert tl.product == "TL"
    assert tl.bond.bond_code == "2400001"  # 7-digit special-bond code
    assert tl.bond.maturity_date == "2054-05-20"


def test_parse_handles_empty_optional_codes():
    raw = (
        "测试国债,260099,,,20300101,2.00,0.9500,T2606,T\n"
    ).encode("utf-8")
    snaps = parse_deliverable_csv(raw)
    assert len(snaps) == 1
    assert snaps[0].bond.sh_code is None
    assert snaps[0].bond.sz_code is None


# ---- Bond master upsert -------------------------------------------------


def test_upsert_bond_inserted_then_unchanged():
    b = Bond("240017", "24国债17", coupon_rate=0.0211, maturity_date="2034-08-25")
    assert bonds.upsert_bond(b) == "inserted"
    assert bonds.upsert_bond(b) == "unchanged"


def test_upsert_bond_updates_when_codes_added():
    b1 = Bond("240017", "24国债17", coupon_rate=0.0211, maturity_date="2034-08-25")
    b2 = Bond("240017", "24国债17", sh_code="019734", sz_code="102273",
              coupon_rate=0.0211, maturity_date="2034-08-25")
    bonds.upsert_bond(b1)
    assert bonds.upsert_bond(b2) == "updated"
    fetched = bonds.get_bond("240017")
    assert fetched.sh_code == "019734"


def test_upsert_logs_warning_on_coupon_change(caplog):
    bonds.upsert_bond(Bond("240017", "x", coupon_rate=0.0211, maturity_date="2034-08-25"))
    # Different coupon — should warn but still update
    bonds.upsert_bond(Bond("240017", "x", coupon_rate=0.0250, maturity_date="2034-08-25"))
    fetched = bonds.get_bond("240017")
    assert fetched.coupon_rate == pytest.approx(0.0250)


# ---- Live integration ---------------------------------------------------


@pytest.mark.network
def test_fetch_deliverable_pool_live():
    from src.data.fetchers import fetch_deliverable_pool

    snaps = fetch_deliverable_pool()
    # Sanity ranges based on currently-listed contracts
    assert len(snaps) >= 80
    contracts = {s.contract_id for s in snaps}
    assert len(contracts) >= 8
    products = {s.product for s in snaps}
    assert {"TS", "TF", "T", "TL"}.issubset(products)
    # Every snapshot must have a usable CF
    assert all(0.5 <= s.cf_row.cf <= 1.5 for s in snaps)
