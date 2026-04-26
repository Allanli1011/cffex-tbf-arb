"""Tests for the CF append-only table and CFFEX scraper."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.data import cf_table, storage
from src.data.cf_table import CFConflictError, CFRow


@pytest.fixture(autouse=True)
def fresh_schema(tmp_path, monkeypatch):
    """Use a per-test SQLite file so tests don't pollute each other."""
    db = tmp_path / "test.db"
    monkeypatch.setattr(storage, "SQLITE_PATH", db)
    storage.init_schema()
    yield
    # cleanup happens via tmp_path teardown


# ---- CFRow validation ---------------------------------------------------


def test_cfrow_validate_ok():
    CFRow("T2509", "240017.IB", 0.95).validate()


def test_cfrow_validate_missing_id():
    with pytest.raises(ValueError):
        CFRow("", "240017.IB", 0.95).validate()


def test_cfrow_validate_cf_out_of_range():
    with pytest.raises(ValueError):
        CFRow("T2509", "240017.IB", 5.0).validate()


# ---- Insert / append-only -----------------------------------------------


def test_insert_new_row():
    row = CFRow("T2509", "240017.IB", 0.9512)
    assert cf_table.insert_cf(row) == "inserted"
    assert cf_table.get_cf("T2509", "240017.IB") == pytest.approx(0.9512)


def test_idempotent_re_insert_unchanged():
    row = CFRow("T2509", "240017.IB", 0.9512)
    cf_table.insert_cf(row)
    assert cf_table.insert_cf(row) == "unchanged"


def test_conflict_raises():
    cf_table.insert_cf(CFRow("T2509", "240017.IB", 0.9512))
    with pytest.raises(CFConflictError):
        cf_table.insert_cf(CFRow("T2509", "240017.IB", 0.9700))


def test_bulk_insert_rolls_back_on_conflict():
    cf_table.insert_cf(CFRow("T2509", "240017.IB", 0.9512))
    rows = [
        CFRow("T2512", "240017.IB", 0.9550),  # would succeed
        CFRow("T2509", "240017.IB", 0.9999),  # conflict
    ]
    with pytest.raises(CFConflictError):
        cf_table.insert_cfs(rows)
    # The good row must NOT have been written, since validation runs first
    assert cf_table.get_cf("T2512", "240017.IB") is None


def test_bulk_insert_mixed_outcomes():
    cf_table.insert_cf(CFRow("T2509", "240017.IB", 0.9512))
    rows = [
        CFRow("T2509", "240017.IB", 0.9512),  # unchanged
        CFRow("T2512", "240017.IB", 0.9550),  # inserted
    ]
    result = cf_table.insert_cfs(rows)
    assert result == {"inserted": 1, "unchanged": 1}


# ---- CSV roundtrip ------------------------------------------------------


def test_csv_export_then_import(tmp_path: Path):
    cf_table.insert_cf(
        CFRow(
            "T2509",
            "240017.IB",
            0.9512,
            bond_name="24国债17",
            coupon_rate=0.0235,
            maturity_date="2034-08-15",
            announce_date="2025-09-01",
            source_url="http://example",
        )
    )
    out = tmp_path / "cf.csv"
    n = cf_table.export_csv(out)
    assert n == 1
    assert out.exists()

    # Wipe and re-import
    with storage.sqlite_conn() as conn:
        conn.execute("DELETE FROM conversion_factors")
    result = cf_table.import_csv(out)
    assert result == {"inserted": 1, "unchanged": 0}
    assert cf_table.get_cf("T2509", "240017.IB") == pytest.approx(0.9512)


# ---- Scraper parsing (unit, no network) ---------------------------------


def test_parse_incremental_sample():
    from src.data.cffex_scraper import (
        AnnouncementRef,
        TITLE_INCR_RE,
        parse_incremental,
    )

    ref = AnnouncementRef(
        url="http://example/47620.html",
        title="关于增加5年期国债期货合约可交割国债的通知",
        publish_date="2026-04-17",
    )
    body = (
        "中金所发〔2026〕15号 各会员单位：\n"
        "2026年记账式附息（八期）国债已招标发行。根据《中国金融期货交易所国债期货"
        "合约交割细则》及相关规定，该国债符合TF2606、TF2609和TF2612合约的可交割国债"
        "条件，转换因子分别为0.9334、0.9366和0.9398。"
    )
    assert TITLE_INCR_RE.search(ref.title)
    parsed = parse_incremental(ref, body)
    assert parsed is not None
    assert parsed.bond_name == "2026年记账式附息（八期）国债"
    assert parsed.contracts == ["TF2606", "TF2609", "TF2612"]
    assert parsed.cfs == [0.9334, 0.9366, 0.9398]
    rows = parsed.to_cf_rows()
    assert len(rows) == 3
    assert all(r.cf for r in rows)


def test_parse_incremental_single_contract():
    from src.data.cffex_scraper import AnnouncementRef, parse_incremental

    ref = AnnouncementRef(
        url="http://example/x.html",
        title="关于增加10年期国债期货合约可交割国债的通知",
        publish_date="2026-03-27",
    )
    body = (
        "2026年记账式附息（五期）国债符合T2612合约的可交割国债条件，"
        "转换因子为0.8895。"
    )
    parsed = parse_incremental(ref, body)
    assert parsed is not None
    assert parsed.contracts == ["T2612"]
    assert parsed.cfs == [0.8895]


def test_filter_cf_announcements():
    from src.data.cffex_scraper import AnnouncementRef, filter_cf_announcements

    refs = [
        AnnouncementRef("u1", "关于增加5年期国债期货合约可交割国债的通知", "2026-04-17"),
        AnnouncementRef("u2", "国债期货新合约上市通知", "2026-03-13"),
        AnnouncementRef("u3", "关于发布国债期货合约可交割国债的通知", "2026-03-13"),
        AnnouncementRef("u4", "股指期货和股指期权合约交割的通知", "2026-04-17"),
    ]
    out = filter_cf_announcements(refs)
    urls = [r.url for r in out]
    assert "u1" in urls
    assert "u3" in urls
    assert "u2" not in urls
    assert "u4" not in urls


# ---- Live integration (network) -----------------------------------------


@pytest.mark.network
def test_cffex_listing_live():
    """Hit the live CFFEX index page; should return some announcements."""
    from src.data.cffex_scraper import list_announcements

    refs = list_announcements()
    assert len(refs) > 0
    # at least one URL should be a /jystz/ detail page
    assert any("/jystz/" in r.url for r in refs)
